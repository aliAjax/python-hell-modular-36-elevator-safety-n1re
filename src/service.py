import hashlib
import json
from datetime import datetime
from uuid import uuid4

from .audit import AuditTrail
from .domain import Actor, ConflictError, NotFoundError, PermissionDenied, ValidationError
from .rules import RuleEngine

# Batch upload is authorized once at the sync boundary; business-rule checks
# during merge run as the system while audit entries keep the real uploader.
SYSTEM_ACTOR = Actor("system:sync", "admin")


RESCUE_STATUS_RANK = {"dispatched": 0, "on_site": 1, "completed": 2}
MAINT_STATUS_RANK = {"planned": 0, "in_progress": 1, "completed": 2}
CLOSED_ALARM_STATUSES = ("closed", "false_alarm")
SYNC_ROLES = ("admin", "dispatcher", "maintenance", "inspector")


def _digest(source_id, records):
    canonical = json.dumps(
        {"source_id": source_id, "records": records},
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _stable_id(prefix, source_id, record_id):
    digest = hashlib.sha256((source_id + "\0" + record_id).encode("utf-8")).hexdigest()[:24]
    return prefix + digest


def _is_blank(value):
    return value is None or value == "" or value == [] or value == {}


def _iso_timestamp(value, field):
    if _is_blank(value):
        return
    try:
        datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        raise ValidationError(field + " must be ISO-8601")


def absorb_fields(current, patch, source):
    """Fill only blank fields; neither side overwrites the other.

    ``field_sources`` records where each populated value came from so the
    ownership of arrival time, rescue outcome, etc. stays auditable.
    """
    merged = dict(current)
    sources = dict(merged.get("field_sources", {}))
    for key, value in patch.items():
        if _is_blank(value):
            continue
        if _is_blank(merged.get(key)):
            merged[key] = value
            sources[key] = source
        elif key not in sources:
            sources[key] = "central" if source == "field" else "field"
    merged["field_sources"] = sources
    return merged


def rank_status(ranks, current, candidate):
    """Return the furthest status, never moving an entity backwards."""
    if candidate not in ranks:
        return current
    if ranks.get(current, -1) < ranks[candidate]:
        return candidate
    return current


class DomainService:
    def __init__(self, repository, rules=None):
        self.repository = repository
        self.rules = rules or RuleEngine()
        self.audit = AuditTrail(repository)

    def _lookup(self, kind, field, value):
        return self.repository.find_entities(self.rules.normalize_kind(kind), field, value)

    def _lookup_conn(self, conn, kind, field, value):
        return self.repository.find_entities(self.rules.normalize_kind(kind), field, value, conn=conn)

    def health(self):
        return {"status": "ok" if self.repository.ping() else "error"}

    def create(self, actor, kind, data, idempotency_key=None):
        kind = self.rules.normalize_kind(kind)
        payload = dict(data or {})
        if idempotency_key:
            existing = self.repository.get_idempotency(actor.user_id, idempotency_key)
            if existing:
                entity = self.repository.get_entity(existing)
                if entity:
                    return entity
        self.rules.validate_create(actor, kind, payload, self._lookup)
        entity_id = str(payload.pop("id", "") or uuid4())
        if self.repository.get_entity(entity_id):
            raise ConflictError("entity already exists: " + entity_id)
        status = self.rules.initial_status(kind, payload)
        entity = self.repository.create_entity(entity_id, kind, status, payload, actor.user_id)
        self.audit.record(entity_id, actor, "create", None, status, {"kind": kind})
        if idempotency_key:
            self.repository.save_idempotency(actor.user_id, idempotency_key, entity_id)
        return entity

    def transition(self, actor, entity_id, action, data=None, expected_version=None):
        entity = self.repository.get_entity(entity_id)
        if not entity:
            raise NotFoundError("entity not found: " + entity_id)
        expected = int(expected_version) if expected_version is not None else entity["version"]
        next_status, patch = self.rules.validate_transition(
            actor, entity, action, dict(data or {}), self._lookup
        )
        merged = absorb_fields(entity["data"], patch, "central")
        kind = entity["kind"]
        # Equipment leaving service voids every outstanding permit; the side
        # effect is committed in the same transaction as the status change.
        if kind == "equipment" and action in ("suspend", "out_of_service"):
            updated = self._transition_with_permit_revocation(
                actor, entity, expected, next_status, merged, action
            )
        else:
            updated = self.repository.update_entity(entity_id, expected, next_status, merged)
            self.audit.record(
                entity_id, actor, action, entity["status"], updated["status"], {"patch": patch}
            )
        return updated

    def _transition_with_permit_revocation(self, actor, equipment, expected, next_status, merged_data, action):
        with self.repository.transaction() as conn:
            updated = self.repository.update_entity(
                equipment["id"], expected, next_status, merged_data, conn=conn
            )
            self.repository.append_audit(
                equipment["id"], actor.user_id, actor.role, action,
                equipment["status"], next_status, {"patch": merged_data}, conn=conn,
            )
            permits = [
                p
                for p in self.repository.list_entities(kind="permit", conn=conn)
                if p["data"].get("equipment_id") == equipment["id"]
                and p["status"] in ("granted", "pending_review")
            ]
            for permit in permits:
                permit_data = dict(permit["data"])
                permit_data.update({
                    "revoked_by": "system",
                    "reason": "equipment %s: old permit voided by status change" % action,
                })
                self.repository.update_entity(
                    permit["id"], permit["version"], "revoked", permit_data, conn=conn
                )
                self.repository.append_audit(
                    permit["id"], actor.user_id, actor.role, "auto_revoke",
                    permit["status"], "revoked",
                    {"equipment_id": equipment["id"], "trigger": action}, conn=conn,
                )
        return updated

    # ------------------------------------------------------------------
    # Offline batch synchronization
    # ------------------------------------------------------------------

    def get_sync_batch(self, batch_id):
        batch = self.repository.get_sync_batch(batch_id)
        if not batch:
            raise NotFoundError("sync batch not found: " + batch_id)
        return batch

    def sync_batch(self, actor, batch_id, source_id, records):
        """Merge one field-uploaded batch into the central store.

        - Same batch id + identical payload is a retry of an upload and lands
          in the store exactly once.
        - The whole batch merges in one transaction; any failure rolls the
          records back while the original batch is retained as ``failed`` for
          another retry.
        """
        if actor.role not in SYNC_ROLES:
            raise PermissionDenied("role %s cannot sync offline batches" % actor.role)
        batch_id = str(batch_id or "").strip()
        source_id = str(source_id or "").strip()
        if not batch_id:
            raise ValidationError("batch_id is required")
        if not source_id:
            raise ValidationError("source_id is required")
        if not isinstance(records, list) or not records:
            raise ValidationError("records must be a non-empty list")
        for raw in records:
            if not isinstance(raw, dict):
                raise ValidationError("each offline record must be an object")
            if _is_blank(raw.get("record_id")):
                raise ValidationError("record_id is required")

        digest = _digest(source_id, records)
        existing = self.repository.get_sync_batch(batch_id)
        if existing and existing["status"] == "merged":
            if existing["digest"] != digest:
                raise ConflictError("batch %s already merged with different content" % batch_id)
            return existing
        if existing and existing["digest"] != digest:
            raise ConflictError("batch %s already retained with different content" % batch_id)

        try:
            with self.repository.transaction() as conn:
                results = self._merge_records(actor, conn, batch_id, source_id, records)
                batch = self.repository.save_sync_batch(
                    {
                        "batch_id": batch_id,
                        "source_id": source_id,
                        "status": "merged",
                        "digest": digest,
                        "records": records,
                        "result": {"items": results},
                        "error": None,
                    },
                    conn=conn,
                )
        except Exception as exc:
            # Keep the original batch verbatim so the field side can retry it.
            self.repository.save_sync_batch(
                {
                    "batch_id": batch_id,
                    "source_id": source_id,
                    "status": "failed",
                    "digest": digest,
                    "records": records,
                    "result": None,
                    "error": "%s: %s" % (type(exc).__name__, exc),
                    "created_at": existing["created_at"] if existing else None,
                }
            )
            if isinstance(exc, (ValidationError, ConflictError, NotFoundError, PermissionDenied)):
                raise ConflictError(
                    "sync batch %s failed and was retained for retry: %s" % (batch_id, exc)
                )
            raise
        return batch

    def _merge_records(self, actor, conn, batch_id, source_id, records):
        # Reference data (alarms/equipment) must land before dependent records.
        ordered = sorted(records, key=lambda r: 0 if r.get("type") == "alarm" else 1)
        results = []
        for raw in ordered:
            record_id = str(raw["record_id"])
            ledger_id = _stable_id("offline-record-", source_id, record_id)
            if self.repository.get_entity(ledger_id, conn=conn):
                results.append({"record_id": record_id, "action": "skipped_duplicate"})
                continue
            record_type = raw.get("type")
            if record_type == "alarm":
                target = self._merge_alarm(actor, conn, batch_id, source_id, record_id, raw)
            elif record_type == "rescue":
                target = self._merge_rescue(actor, conn, batch_id, source_id, record_id, raw)
            elif record_type == "maintenance":
                target = self._merge_maintenance(actor, conn, batch_id, source_id, record_id, raw)
            else:
                raise ValidationError("unknown offline record type: %s" % record_type)
            self.repository.create_entity(
                ledger_id,
                "offline_record",
                "merged",
                {
                    "batch_id": batch_id,
                    "source_id": source_id,
                    "record_id": record_id,
                    "type": record_type,
                    "target_id": target["id"],
                    "raw": raw,
                },
                actor.user_id,
                conn=conn,
            )
            self.repository.append_audit(
                ledger_id, actor.user_id, actor.role, "sync_merge",
                None, "merged",
                {"batch_id": batch_id, "source_id": source_id, "record_id": record_id,
                 "target_id": target["id"], "target_kind": target["kind"]},
                conn=conn,
            )
            results.append({
                "record_id": record_id,
                "action": target["_sync_action"],
                "kind": target["kind"],
                "target_id": target["id"],
                "status": target["status"],
            })
        return results

    def _find_equipment(self, conn, equipment_id):
        equipment = None
        if equipment_id:
            equipment = self.repository.get_entity(equipment_id, conn=conn)
        if not equipment or equipment["kind"] != "equipment":
            raise ValidationError("offline record references unknown equipment: %s" % equipment_id)
        return equipment

    def _find_alarm(self, conn, equipment_id, code):
        alarms = [
            a
            for a in self.repository.list_entities(kind="alarm", conn=conn)
            if a["data"].get("equipment_id") == equipment_id and a["data"].get("code") == code
        ]
        active = [a for a in alarms if a["status"] not in CLOSED_ALARM_STATUSES]
        if active:
            return active[0]
        if alarms:
            # Late upload against an already-closed alarm: attach, never reopen.
            return alarms[-1]
        return None

    def _merge_alarm(self, actor, conn, batch_id, source_id, record_id, raw):
        equipment_id = raw.get("equipment_id")
        code = raw.get("code")
        if _is_blank(equipment_id) or _is_blank(code):
            raise ValidationError("alarm record requires equipment_id and code")
        _iso_timestamp(raw.get("occurred_at"), "occurred_at")
        self._find_equipment(conn, equipment_id)
        patch = {k: raw.get(k) for k in ("code", "occurred_at", "notes") if not _is_blank(raw.get(k))}

        alarm = self._find_alarm(conn, equipment_id, code)
        if alarm:
            # Attaching to a closed alarm updates data only; it is never reopened.
            merged_data = absorb_fields(alarm["data"], patch, "field")
            updated = self.repository.update_entity(
                alarm["id"], alarm["version"], alarm["status"], merged_data, conn=conn
            )
            self.repository.append_audit(
                alarm["id"], actor.user_id, actor.role, "sync_merge",
                alarm["status"], alarm["status"],
                {"batch_id": batch_id, "source_id": source_id, "record_id": record_id, "patch": patch},
                conn=conn,
            )
            updated["_sync_action"] = "merged"
            return updated

        # First sighting anywhere: the field record becomes the central alarm.
        payload = {
            "equipment_id": equipment_id,
            "code": code,
            "occurred_at": raw.get("occurred_at"),
            "source": "field",
            "field_sources": {k: "field" for k in patch},
        }
        payload = {k: v for k, v in payload.items() if not _is_blank(v) or k == "field_sources"}
        self.rules.validate_create(SYSTEM_ACTOR, "alarm", dict(payload), lambda k, f, v: self._lookup_conn(conn, k, f, v))
        entity_id = _stable_id("offline-alarm-", source_id, record_id)
        if self.repository.get_entity(entity_id, conn=conn):
            raise ConflictError("alarm entity id collision: " + entity_id)
        created = self.repository.create_entity(
            entity_id, "alarm", self.rules.initial_status("alarm"), payload, actor.user_id, conn=conn
        )
        self.repository.append_audit(
            entity_id, actor.user_id, actor.role, "sync_create",
            None, created["status"],
            {"batch_id": batch_id, "source_id": source_id, "record_id": record_id},
            conn=conn,
        )
        created["_sync_action"] = "created"
        return created

    def _merge_rescue(self, actor, conn, batch_id, source_id, record_id, raw):
        equipment_id = raw.get("equipment_id")
        code = raw.get("code")
        if _is_blank(equipment_id) or _is_blank(code):
            raise ValidationError("rescue record requires equipment_id and code")
        _iso_timestamp(raw.get("arrived_at"), "arrived_at")
        alarm = self._find_alarm(conn, equipment_id, code)
        if not alarm:
            raise ConflictError(
                "rescue record %s has no alarm for equipment %s code %s in this batch or center"
                % (record_id, equipment_id, code)
            )

        dedupe_key = raw.get("dedupe_key") or ("field:" + source_id + ":" + record_id)
        team = raw.get("team") or "field-team"
        candidate_status = raw.get("status") or ("completed" if raw.get("outcome") else "on_site")
        patch = {
            "arrived_at": raw.get("arrived_at"),
            "outcome": raw.get("outcome"),
        }
        patch = {k: v for k, v in patch.items() if not _is_blank(v)}

        jobs = [
            j
            for j in self.repository.list_entities(kind="rescue_job", conn=conn)
            if j["data"].get("alarm_id") == alarm["id"]
        ]
        open_jobs = [j for j in jobs if j["status"] in RESCUE_STATUS_RANK]
        job = next((j for j in open_jobs if j["data"].get("dedupe_key") == dedupe_key), None)
        if job is None and open_jobs:
            job = open_jobs[0]

        if job:
            merged_data = absorb_fields(job["data"], patch, "field")
            if job["status"] in RESCUE_STATUS_RANK:
                next_status = rank_status(RESCUE_STATUS_RANK, job["status"], candidate_status)
            else:
                next_status = job["status"]
            updated = self.repository.update_entity(
                job["id"], job["version"], next_status, merged_data, conn=conn
            )
            self.repository.append_audit(
                job["id"], actor.user_id, actor.role, "sync_merge",
                job["status"], next_status,
                {"batch_id": batch_id, "source_id": source_id, "record_id": record_id, "patch": patch},
                conn=conn,
            )
            updated["_sync_action"] = "merged"
            return updated

        payload = {
            "alarm_id": alarm["id"],
            "dedupe_key": dedupe_key,
            "team": team,
            "source": "field",
            "field_sources": {k: "field" for k in patch},
        }
        payload.update(patch)
        self.rules.validate_create(
            SYSTEM_ACTOR, "rescue_job", dict(payload), lambda k, f, v: self._lookup_conn(conn, k, f, v)
        )
        entity_id = _stable_id("offline-job-", source_id, record_id)
        if self.repository.get_entity(entity_id, conn=conn):
            raise ConflictError("rescue job id collision: " + entity_id)
        initial = self.rules.initial_status("rescue_job")
        status = rank_status(RESCUE_STATUS_RANK, initial, candidate_status)
        created = self.repository.create_entity(
            entity_id, "rescue_job", status, payload, actor.user_id, conn=conn
        )
        self.repository.append_audit(
            entity_id, actor.user_id, actor.role, "sync_create",
            None, status,
            {"batch_id": batch_id, "source_id": source_id, "record_id": record_id},
            conn=conn,
        )
        created["_sync_action"] = "created"
        return created

    def _merge_maintenance(self, actor, conn, batch_id, source_id, record_id, raw):
        equipment_id = raw.get("equipment_id")
        work_type = raw.get("work_type")
        if _is_blank(equipment_id) or _is_blank(work_type):
            raise ValidationError("maintenance record requires equipment_id and work_type")
        _iso_timestamp(raw.get("planned_at"), "planned_at")
        _iso_timestamp(raw.get("completed_at"), "completed_at")
        self._find_equipment(conn, equipment_id)

        candidate_status = raw.get("status") or (
            "completed" if raw.get("completed_at") else "in_progress"
        )
        patch = {
            "planned_at": raw.get("planned_at"),
            "completed_at": raw.get("completed_at"),
            "summary": raw.get("summary"),
            "part_serial": raw.get("part_serial"),
        }
        patch = {k: v for k, v in patch.items() if not _is_blank(v)}

        entity_id = _stable_id("offline-maint-", source_id, record_id)
        job = self.repository.get_entity(entity_id, conn=conn)
        if not job:
            # Match a central open maintenance record for the same equipment +
            # work type before falling back to the deterministic offline id.
            candidates = [
                m
                for m in self.repository.list_entities(kind="maintenance", conn=conn)
                if m["data"].get("equipment_id") == equipment_id
                and m["data"].get("work_type") == work_type
                and m["status"] in MAINT_STATUS_RANK
            ]
            job = candidates[0] if candidates else None

        if job:
            merged_data = absorb_fields(job["data"], patch, "field")
            next_status = rank_status(MAINT_STATUS_RANK, job["status"], candidate_status)
            updated = self.repository.update_entity(
                job["id"], job["version"], next_status, merged_data, conn=conn
            )
            self.repository.append_audit(
                job["id"], actor.user_id, actor.role, "sync_merge",
                job["status"], next_status,
                {"batch_id": batch_id, "source_id": source_id, "record_id": record_id, "patch": patch},
                conn=conn,
            )
            updated["_sync_action"] = "merged"
            return updated

        payload = {
            "equipment_id": equipment_id,
            "work_type": work_type,
            "planned_at": raw.get("planned_at"),
            "source": "field",
            "field_sources": {k: "field" for k in patch},
        }
        payload.update(patch)
        payload = {k: v for k, v in payload.items() if not _is_blank(v) or k == "field_sources"}
        self.rules.validate_create(
            SYSTEM_ACTOR, "maintenance", dict(payload), lambda k, f, v: self._lookup_conn(conn, k, f, v)
        )
        initial = self.rules.initial_status("maintenance")
        status = rank_status(MAINT_STATUS_RANK, initial, candidate_status)
        created = self.repository.create_entity(
            entity_id, "maintenance", status, payload, actor.user_id, conn=conn
        )
        self.repository.append_audit(
            entity_id, actor.user_id, actor.role, "sync_create",
            None, status,
            {"batch_id": batch_id, "source_id": source_id, "record_id": record_id},
            conn=conn,
        )
        created["_sync_action"] = "created"
        return created

    def get(self, entity_id):
        entity = self.repository.get_entity(entity_id)
        if not entity:
            raise NotFoundError("entity not found: " + entity_id)
        return entity

    def list(self, kind=None, status=None):
        if kind:
            kind = self.rules.normalize_kind(kind)
        return self.repository.list_entities(kind=kind, status=status)

    def list_sync_batches(self):
        with self.repository._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM sync_batches ORDER BY created_at, batch_id"
            ).fetchall()
        return [self.repository._batch_from_row(row) for row in rows]

    def audit_log(self, entity_id=None):
        return self.repository.list_audit(entity_id=entity_id)
