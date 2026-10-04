import hashlib
import json
from datetime import datetime, timezone
from uuid import uuid4

from .audit import AuditTrail
from .domain import ConflictError, NotFoundError, PermissionDenied, ValidationError
from .rules import RuleEngine

_OFFLINE_KINDS = ("alarm", "rescue_job", "maintenance")
_OFFLINE_META = {
    "id",
    "kind",
    "source_id",
    "record_id",
    "batch_id",
    "status",
    "created_at",
    "updated_at",
    "version",
    "created_by",
}


def _utcnow():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class DomainService:
    def __init__(self, repository, rules=None):
        self.repository = repository
        self.rules = rules or RuleEngine()
        self.audit = AuditTrail(repository)

    def _lookup(self, kind, field, value):
        return self.repository.find_entities(self.rules.normalize_kind(kind), field, value)

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
        merged = dict(entity["data"])
        merged.update(patch)
        updated = self.repository.update_entity(entity_id, expected, next_status, merged)
        self.audit.record(
            entity_id,
            actor,
            action,
            entity["status"],
            updated["status"],
            {"patch": patch},
        )
        if entity["kind"] == "equipment" and action in ("suspend", "out_of_service"):
            self._revoke_permits_for_equipment(entity_id, actor)
        return updated

    def _revoke_permits_for_equipment(self, equipment_id, actor):
        """A status change voids every active recovery permit for the equipment."""
        for permit in self.repository.list_entities(kind="permit"):
            if permit["data"].get("equipment_id") != equipment_id:
                continue
            if permit["status"] not in ("granted", "pending_review"):
                continue
            merged = dict(permit["data"])
            merged["revoked_reason"] = "equipment status changed"
            merged["revoked_at"] = _utcnow()
            self.repository.update_entity(permit["id"], permit["version"], "revoked", merged)
            self.audit.record(
                permit["id"],
                actor,
                "revoke",
                permit["status"],
                "revoked",
                {"reason": "equipment status changed", "equipment_id": equipment_id},
            )

    def _batch_id(self, records):
        digest = hashlib.sha256(
            json.dumps(records, ensure_ascii=False, sort_keys=True).encode("utf-8")
        ).hexdigest()[:16]
        return "batch-" + digest

    def list_batches(self):
        return self.repository.list_batches()

    def merge_offline(self, actor, records, batch_id=None):
        """Merge offline-registered alarms, rescue jobs and maintenance into the central DB.

        Records are uploaded in batches keyed by ``batch_id``. Re-uploading the same batch
        is idempotent: a merged batch returns its stored result without touching the data
        again, and a failed batch keeps its original payload so it can be retried later.

        Merges are field-level: fields the centre already holds are never overwritten, and
        fields recorded on site (e.g. arrival time) are filled in where the centre has no
        value, so neither side stamps over the other. Status is never taken from an offline
        record, so an alarm cannot be closed before it is resolved.
        """
        if not isinstance(records, list):
            raise ValidationError("records must be a list")
        for raw in records:
            if not isinstance(raw, dict):
                raise ValidationError("each offline record must be an object")
        batch_id = str(batch_id or "").strip() or self._batch_id(records)

        existing = self.repository.get_batch(batch_id)
        if existing:
            if existing["status"] == "merged":
                return existing["result"]
            records = existing["payload"]
        else:
            self.repository.create_batch(batch_id, actor.user_id, records)

        try:
            result = self._merge_records(actor, batch_id, records)
        except Exception as exc:
            self.repository.mark_batch(batch_id, "failed", error=str(exc))
            raise
        self.repository.mark_batch(batch_id, "merged", result=result)
        return result

    def _merge_records(self, actor, batch_id, records):
        results = []
        for raw in records:
            kind = self.rules.normalize_kind(str(raw.get("kind", "")).strip())
            if kind not in _OFFLINE_KINDS:
                raise ValidationError("unsupported offline record kind: " + str(kind))
            target = self._find_merge_target(kind, raw)
            if target:
                results.append(self._merge_into(actor, batch_id, kind, target, raw))
            else:
                results.append(self._create_from_offline(actor, batch_id, kind, raw))
        return {"batch_id": batch_id, "count": len(results), "items": results}

    def _find_merge_target(self, kind, raw):
        if kind == "alarm":
            equipment_id = raw.get("equipment_id")
            code = raw.get("code")
            if not equipment_id or not code:
                return None
            alarms = self.repository.list_entities(kind="alarm")
            for alarm in alarms:
                if (
                    alarm["data"].get("equipment_id") == equipment_id
                    and alarm["data"].get("code") == code
                    and alarm["status"] not in ("closed", "false_alarm")
                ):
                    return alarm
            for alarm in alarms:
                if (
                    alarm["data"].get("equipment_id") == equipment_id
                    and alarm["data"].get("code") == code
                ):
                    return alarm
            return None
        if kind == "rescue_job":
            dedupe = raw.get("dedupe_key")
            if dedupe:
                for job in self.repository.list_entities(kind="rescue_job"):
                    if job["data"].get("dedupe_key") == dedupe:
                        return job
            alarm_id = raw.get("alarm_id")
            if alarm_id:
                for job in self.repository.list_entities(kind="rescue_job"):
                    if job["data"].get("alarm_id") == alarm_id and job["status"] not in (
                        "completed",
                        "aborted",
                    ):
                        return job
            return None
        if kind == "maintenance":
            equipment_id = raw.get("equipment_id")
            planned_at = raw.get("planned_at")
            if equipment_id and planned_at:
                for item in self.repository.list_entities(kind="maintenance"):
                    if (
                        item["data"].get("equipment_id") == equipment_id
                        and item["data"].get("planned_at") == planned_at
                    ):
                        return item
        return None

    def _resolve_alarm_id(self, raw):
        if raw.get("alarm_id"):
            return raw["alarm_id"]
        equipment_id = raw.get("equipment_id")
        code = raw.get("code")
        if equipment_id and code:
            for alarm in self.repository.list_entities(kind="alarm"):
                if (
                    alarm["data"].get("equipment_id") == equipment_id
                    and alarm["data"].get("code") == code
                ):
                    return alarm["id"]
        return None

    def _offline_fields(self, raw):
        fields = {}
        for key, value in raw.items():
            if key in _OFFLINE_META:
                continue
            if value is None or value == "" or value == [] or value == {}:
                continue
            fields[key] = value
        return fields

    def _merge_into(self, actor, batch_id, kind, target, raw):
        target = self.repository.get_entity(target["id"])
        merged = dict(target["data"])
        added = []
        for key, value in self._offline_fields(raw).items():
            if merged.get(key) not in (None, "", [], {}):
                continue
            merged[key] = value
            added.append(key)
        updated = self.repository.update_entity(target["id"], target["version"], target["status"], merged)
        self.audit.record(
            target["id"],
            actor,
            "merge_offline",
            target["status"],
            updated["status"],
            {
                "kind": kind,
                "batch_id": batch_id,
                "added": added,
                "source_id": raw.get("source_id"),
                "record_id": raw.get("record_id"),
            },
        )
        return updated

    def _create_from_offline(self, actor, batch_id, kind, raw):
        payload = self._offline_fields(raw)
        if kind == "rescue_job" and not payload.get("alarm_id"):
            alarm_id = self._resolve_alarm_id(raw)
            if alarm_id:
                payload["alarm_id"] = alarm_id
        self.rules.validate_create(actor, kind, payload, self._lookup)
        entity_id = str(uuid4())
        status = self.rules.initial_status(kind, payload)
        entity = self.repository.create_entity(entity_id, kind, status, payload, actor.user_id)
        self.audit.record(
            entity_id,
            actor,
            "merge_offline",
            None,
            status,
            {
                "kind": kind,
                "batch_id": batch_id,
                "source_id": raw.get("source_id"),
                "record_id": raw.get("record_id"),
            },
        )
        return entity

    def get(self, entity_id):
        entity = self.repository.get_entity(entity_id)
        if not entity:
            raise NotFoundError("entity not found: " + entity_id)
        return entity

    def list(self, kind=None, status=None):
        if kind:
            kind = self.rules.normalize_kind(kind)
        return self.repository.list_entities(kind=kind, status=status)

    def audit_log(self, entity_id=None):
        return self.repository.list_audit(entity_id=entity_id)
