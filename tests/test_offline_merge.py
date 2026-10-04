import tempfile
import unittest
from pathlib import Path

from src.domain import Actor, ConflictError, ValidationError
from src.repository import SQLiteRepository
from src.rules import RuleEngine
from src.service import DomainService


class OfflineMergeTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.service = DomainService(SQLiteRepository(Path(self.tmp.name) / "test.db"), RuleEngine())
        self.actor = Actor("admin", "admin")

    def tearDown(self):
        self.tmp.cleanup()

    def create(self, kind, data):
        return self.service.create(self.actor, kind, data)

    def act(self, entity, action, data=None):
        return self.service.transition(self.actor, entity["id"], action, data or {})

    def equipment(self, asset_no="E-1"):
        return self.create(
            "equipment",
            {
                "asset_no": asset_no,
                "equipment_type": "elevator",
                "location": "A",
                "inspection_interval_days": 365,
            },
        )

    def offline_alarm(self, equipment_id, code="DOOR-JAM", **extra):
        record = {
            "kind": "alarm",
            "source_id": "tablet-1",
            "record_id": "rec-1",
            "equipment_id": equipment_id,
            "code": code,
            "occurred_at": "2026-09-27T10:00:00Z",
        }
        record.update(extra)
        return record

    def test_field_level_merge_preserves_both_sides(self):
        equipment = self.equipment()
        alarm = self.create(
            "alarm",
            {
                "equipment_id": equipment["id"],
                "code": "DOOR-JAM",
                "occurred_at": "2026-09-27T10:00:00Z",
                "rescue_result": "passenger freed by crew",
            },
        )
        result = self.service.merge_offline(
            self.actor,
            [self.offline_alarm(equipment["id"], arrived_at="2026-09-27T10:20:00Z", team="Alpha")],
            "batch-1",
        )
        merged = result["items"][0]
        self.assertEqual(merged["id"], alarm["id"])
        # site-recorded arrival time and centre-recorded rescue result both survive
        self.assertEqual(merged["data"]["arrived_at"], "2026-09-27T10:20:00Z")
        self.assertEqual(merged["data"]["rescue_result"], "passenger freed by crew")
        self.assertEqual(merged["data"]["team"], "Alpha")
        # an alarm is never closed by an offline merge
        self.assertNotEqual(merged["status"], "closed")

    def test_merge_does_not_overwrite_centre_values(self):
        equipment = self.equipment()
        self.create(
            "alarm",
            {
                "equipment_id": equipment["id"],
                "code": "DOOR-JAM",
                "occurred_at": "2026-09-27T10:00:00Z",
                "team": "Centre Team",
            },
        )
        result = self.service.merge_offline(
            self.actor,
            [self.offline_alarm(equipment["id"], team="Site Team")],
            "batch-ow",
        )
        merged = result["items"][0]
        # centre value wins on a field both sides hold
        self.assertEqual(merged["data"]["team"], "Centre Team")

    def test_batch_upload_is_idempotent(self):
        equipment = self.equipment()
        records = [self.offline_alarm(equipment["id"])]
        first = self.service.merge_offline(self.actor, records, "batch-idem")
        second = self.service.merge_offline(self.actor, records, "batch-idem")
        self.assertEqual(first["items"][0]["id"], second["items"][0]["id"])
        self.assertEqual(len(self.service.list("alarm")), 1)
        batches = self.service.list_batches()
        self.assertEqual(len(batches), 1)
        self.assertEqual(batches[0]["status"], "merged")

    def test_batch_without_id_is_idempotent_by_content(self):
        equipment = self.equipment()
        records = [self.offline_alarm(equipment["id"])]
        self.service.merge_offline(self.actor, records)
        self.service.merge_offline(self.actor, records)
        self.assertEqual(len(self.service.list("alarm")), 1)

    def test_failed_batch_is_retained_and_retried(self):
        records = [self.offline_alarm("equip-later")]
        with self.assertRaises(ValidationError):
            self.service.merge_offline(self.actor, records, "batch-retry")
        batch = self.service.repository.get_batch("batch-retry")
        self.assertEqual(batch["status"], "failed")
        self.assertEqual(batch["payload"], records)
        self.assertEqual(batch["attempts"], 1)
        # prerequisite data arrives while the batch is held
        self.create(
            "equipment",
            {
                "id": "equip-later",
                "asset_no": "E-LATER",
                "equipment_type": "elevator",
                "location": "A",
                "inspection_interval_days": 365,
            },
        )
        result = self.service.merge_offline(self.actor, records, "batch-retry")
        self.assertEqual(result["count"], 1)
        batch = self.service.repository.get_batch("batch-retry")
        self.assertEqual(batch["status"], "merged")
        self.assertEqual(batch["attempts"], 2)

    def test_offline_maintenance_merges_by_plan(self):
        equipment = self.equipment()
        maintenance = self.create(
            "maintenance",
            {
                "equipment_id": equipment["id"],
                "work_type": "repair",
                "planned_at": "2026-10-01T09:00:00Z",
                "work_done": "centre note",
            },
        )
        result = self.service.merge_offline(
            self.actor,
            [
                {
                    "kind": "maintenance",
                    "source_id": "tablet-1",
                    "record_id": "m-1",
                    "equipment_id": equipment["id"],
                    "work_type": "repair",
                    "planned_at": "2026-10-01T09:00:00Z",
                    "arrived_at": "2026-10-01T09:05:00Z",
                }
            ],
            "batch-maint",
        )
        merged = result["items"][0]
        self.assertEqual(merged["id"], maintenance["id"])
        self.assertEqual(merged["data"]["work_done"], "centre note")
        self.assertEqual(merged["data"]["arrived_at"], "2026-10-01T09:05:00Z")


class PermitLifecycleTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.service = DomainService(SQLiteRepository(Path(self.tmp.name) / "test.db"), RuleEngine())
        self.actor = Actor("admin", "admin")

    def tearDown(self):
        self.tmp.cleanup()

    def create(self, kind, data):
        return self.service.create(self.actor, kind, data)

    def act(self, entity, action, data=None):
        return self.service.transition(self.actor, entity["id"], action, data or {})

    def _ready_equipment(self, asset_no="E-1"):
        equipment = self.create(
            "equipment",
            {
                "asset_no": asset_no,
                "equipment_type": "elevator",
                "location": "A",
                "inspection_interval_days": 365,
            },
        )
        inspection = self.create(
            "inspection",
            {"equipment_id": equipment["id"], "scheduled_at": "2026-09-27T09:00:00Z", "cycle_days": 365},
        )
        inspection = self.act(inspection, "pass", {"findings": "normal"})
        remediation = self.create(
            "remediation",
            {"equipment_id": equipment["id"], "issue": "door alignment", "owner": "Maint", "due_at": "2026-10-01"},
        )
        remediation = self.act(remediation, "submit_evidence", {"evidence": "IMG-1"})
        remediation = self.act(remediation, "verify", {})
        remediation = self.act(remediation, "close", {})
        permit = self.create(
            "permit",
            {"equipment_id": equipment["id"], "purpose": "return_to_service", "requested_by": "ops"},
        )
        permit = self.act(permit, "request_review", {})
        permit = self.act(permit, "grant", {})
        return equipment, inspection, remediation, permit

    def test_suspending_equipment_revokes_permit(self):
        equipment, _, _, permit = self._ready_equipment()
        self.assertEqual(permit["status"], "granted")
        self.act(equipment, "suspend", {})
        self.assertEqual(self.service.get(permit["id"])["status"], "revoked")

    def test_out_of_service_revokes_permit(self):
        equipment, _, _, permit = self._ready_equipment()
        self.act(equipment, "out_of_service", {})
        self.assertEqual(self.service.get(permit["id"])["status"], "revoked")

    def test_return_to_service_blocked_by_open_remediation(self):
        equipment, _, _, _ = self._ready_equipment()
        # open a fresh remediation and leave it open
        self.create(
            "remediation",
            {"equipment_id": equipment["id"], "issue": "new issue", "owner": "Maint", "due_at": "2026-10-05"},
        )
        self.act(equipment, "suspend", {})
        with self.assertRaises(ConflictError):
            self.act(equipment, "return_to_service", {})

    def test_return_to_service_blocked_by_active_alarm(self):
        equipment, _, _, _ = self._ready_equipment()
        alarm = self.create(
            "alarm",
            {"equipment_id": equipment["id"], "code": "DOOR-JAM", "occurred_at": "2026-09-27T10:00:00Z"},
        )
        self.act(equipment, "suspend", {})
        with self.assertRaises(ConflictError):
            self.act(equipment, "return_to_service", {})
        # resolve and close the alarm, then a fresh permit is still required
        alarm = self.act(alarm, "dispatch", {"team": "Alpha"})
        job = self.create("rescue_job", {"alarm_id": alarm["id"], "dedupe_key": "j1", "team": "Alpha"})
        job = self.act(job, "arrive", {})
        job = self.act(job, "complete", {"outcome": "ok"})
        alarm = self.act(alarm, "resolve", {"resolution": "ok"})
        alarm = self.act(alarm, "close", {})
        with self.assertRaises(ConflictError):
            self.act(equipment, "return_to_service", {})
        # grant a new permit and the return goes through
        permit = self.create(
            "permit",
            {"equipment_id": equipment["id"], "purpose": "return_to_service", "requested_by": "ops"},
        )
        permit = self.act(permit, "request_review", {})
        permit = self.act(permit, "grant", {})
        equipment = self.act(equipment, "return_to_service", {})
        self.assertEqual(equipment["status"], "in_service")


if __name__ == "__main__":
    unittest.main()
