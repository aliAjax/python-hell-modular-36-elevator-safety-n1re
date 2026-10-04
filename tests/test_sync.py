import tempfile
import unittest
from pathlib import Path

from src.domain import Actor, ConflictError
from src.repository import SQLiteRepository
from src.rules import RuleEngine
from src.service import DomainService, absorb_fields, rank_status


class SyncTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.service = DomainService(SQLiteRepository(Path(self.tmp.name) / "test.db"), RuleEngine())
        self.admin = Actor("admin", "admin")
        self.dispatcher = Actor("disp-1", "dispatcher")
        self.field = Actor("field-1", "maintenance")

    def tearDown(self):
        self.tmp.cleanup()

    def equipment(self, asset_no="E-1", actor=None):
        return self.service.create(
            actor or self.admin,
            "equipment",
            {"asset_no": asset_no, "equipment_type": "elevator", "location": "Shaft",
             "inspection_interval_days": 365},
        )

    def batch(self, batch_id, records, actor=None, source_id="tablet-7"):
        return self.service.sync_batch(
            actor or self.field, batch_id, source_id, records
        )

    # 需求1+2：两边各自处理过同一报警，到场时间和救援结果都保留
    def test_center_and_field_both_processed_same_alarm_fields_preserved(self):
        equipment = self.equipment()
        # 中心侧：创建报警、派单、救援完成、中心记录救援结果
        alarm = self.service.create(
            self.dispatcher,
            "alarm",
            {"equipment_id": equipment["id"], "code": "TRAP-01",
             "occurred_at": "2026-10-04T09:00:00Z"},
        )
        alarm = self.service.transition(self.dispatcher, alarm["id"], "dispatch", {"team": "Alpha"})
        job = self.service.create(
            self.dispatcher,
            "rescue_job",
            {"alarm_id": alarm["id"], "dedupe_key": "j-1", "team": "Alpha"},
        )
        self.service.transition(self.dispatcher, job["id"], "arrive", {})
        job = self.service.transition(
            self.dispatcher, job["id"], "complete", {"outcome": "passenger freed, door reset"}
        )
        # 现场离线记录：到场时间 + 同一救援，没写结果
        result = self.batch("b-1", [
            {"record_id": "r-alarm", "type": "alarm", "equipment_id": equipment["id"],
             "code": "TRAP-01", "occurred_at": "2026-10-04T09:02:00Z"},
            {"record_id": "r-rescue", "type": "rescue", "equipment_id": equipment["id"],
             "code": "TRAP-01", "dedupe_key": "j-1", "arrived_at": "2026-10-04T09:11:00Z"},
        ])
        self.assertEqual(result["status"], "merged")

        merged_job = self.service.get(job["id"])
        self.assertEqual(merged_job["data"]["arrived_at"], "2026-10-04T09:11:00Z")
        self.assertEqual(merged_job["data"]["outcome"], "passenger freed, door reset")
        self.assertEqual(merged_job["data"]["field_sources"]["arrived_at"], "field")
        self.assertEqual(merged_job["data"]["field_sources"]["outcome"], "central")
        self.assertEqual(merged_job["status"], "completed")

        # 报警没结之前不能算已关闭：现场记录不会把报警推到 closed
        merged_alarm = self.service.get(alarm["id"])
        self.assertNotIn(merged_alarm["status"], ("closed", "false_alarm"))

    def test_reverse_order_field_first_center_afterward_also_preserved(self):
        equipment = self.equipment()
        # 先离线：报警 + 到场
        self.batch("b-1", [
            {"record_id": "r-alarm", "type": "alarm", "equipment_id": equipment["id"],
             "code": "TRAP-02", "occurred_at": "2026-10-04T09:02:00Z"},
            {"record_id": "r-rescue", "type": "rescue", "equipment_id": equipment["id"],
             "code": "TRAP-02", "arrived_at": "2026-10-04T09:10:00Z"},
        ])
        alarms = [a for a in self.service.list("alarm") if a["data"]["code"] == "TRAP-02"]
        self.assertEqual(len(alarms), 1)
        jobs = self.service.list("rescue_job")
        self.assertEqual(len(jobs), 1)
        # 中心后补救援结果：不得盖掉到到场时间
        job = jobs[0]
        self.service.transition(
            self.dispatcher, job["id"], "complete", {"outcome": "evacuated 3 people"}
        )
        merged_job = self.service.get(job["id"])
        self.assertEqual(merged_job["data"]["arrived_at"], "2026-10-04T09:10:00Z")
        self.assertEqual(merged_job["data"]["outcome"], "evacuated 3 people")

    # 需求3：同一批次重复上传只入库一次
    def test_duplicate_batch_upload_ingests_once(self):
        equipment = self.equipment()
        records = [
            {"record_id": "r1", "type": "maintenance", "equipment_id": equipment["id"],
             "work_type": "repair", "planned_at": "2026-10-04T08:00:00Z",
             "status": "in_progress"},
        ]
        first = self.batch("dup-1", records)
        second = self.batch("dup-1", records)
        self.assertEqual(first["batch_id"], second["batch_id"])
        maintenances = self.service.list("maintenance")
        self.assertEqual(len(maintenances), 1)
        offline = self.service.list("offline_record")
        self.assertEqual(len(offline), 1)
        self.assertEqual(second["result"], first["result"])

    def test_same_batch_id_different_payload_rejected(self):
        equipment = self.equipment()
        self.batch("b-x", [
            {"record_id": "r1", "type": "maintenance", "equipment_id": equipment["id"],
             "work_type": "repair", "planned_at": "2026-10-04T08:00:00Z"},
        ])
        with self.assertRaises(ConflictError):
            self.batch("b-x", [
                {"record_id": "r2", "type": "maintenance", "equipment_id": equipment["id"],
                 "work_type": "routine", "planned_at": "2026-10-04T08:00:00Z"},
            ])

    # 需求3：合并失败保留原批次，原子回滚，修复后重试成功
    def test_failed_batch_is_retained_and_can_retry(self):
        other = self.equipment("E-2")
        bad_records = [
            {"record_id": "r-rescue", "type": "rescue", "equipment_id": other["id"],
             "code": "GHOST", "arrived_at": "2026-10-04T09:10:00Z"},
        ]
        with self.assertRaises(ConflictError):
            self.batch("fail-1", bad_records)
        retained = self.service.get_sync_batch("fail-1")
        self.assertEqual(retained["status"], "failed")
        self.assertEqual(retained["records"], bad_records)
        self.assertIn("GHOST", retained["error"])
        # 没有留下半截数据
        self.assertEqual(self.service.list("rescue_job"), [])
        self.assertEqual(self.service.list("offline_record"), [])

        # 中心补上报警后，用原批次原样重试
        alarm = self.service.create(
            self.dispatcher,
            "alarm",
            {"equipment_id": other["id"], "code": "GHOST",
             "occurred_at": "2026-10-04T09:00:00Z"},
        )
        retried = self.batch("fail-1", bad_records)
        self.assertEqual(retried["status"], "merged")
        jobs = self.service.list("rescue_job")
        self.assertEqual(len(jobs), 1)
        self.assertEqual(jobs[0]["data"]["alarm_id"], alarm["id"])

    def test_batch_merges_atomically_no_partial_ingest(self):
        equipment = self.equipment()
        with self.assertRaises(ConflictError):
            self.batch("atomic-1", [
                {"record_id": "r-maint", "type": "maintenance", "equipment_id": equipment["id"],
                 "work_type": "component_replacement", "planned_at": "2026-10-04T08:00:00Z"},
                {"record_id": "r-bad", "type": "rescue", "equipment_id": equipment["id"],
                 "code": "NOPE", "arrived_at": "2026-10-04T09:10:00Z"},
            ])
        self.assertEqual(self.service.list("maintenance"), [])
        self.assertEqual(self.service.list("rescue_job"), [])
        self.assertEqual(self.service.list("offline_record"), [])

    # 需求4：设备状态一变，旧恢复许可作废；重新启用核对整改与报警
    def test_permit_voided_when_equipment_status_changes(self):
        equipment = self.equipment()
        inspection = self.service.create(
            self.admin, "inspection",
            {"equipment_id": equipment["id"], "scheduled_at": "2026-10-01T09:00:00Z", "cycle_days": 365},
        )
        self.service.transition(self.admin, inspection["id"], "pass", {"findings": "ok"})
        permit = self.service.create(
            self.admin, "permit",
            {"equipment_id": equipment["id"], "purpose": "return_to_service", "requested_by": "ops"},
        )
        permit = self.service.transition(self.admin, permit["id"], "request_review", {})
        permit = self.service.transition(self.admin, permit["id"], "grant", {})
        self.assertEqual(permit["status"], "granted")

        suspended = self.service.transition(self.admin, equipment["id"], "suspend", {})
        self.assertEqual(suspended["status"], "suspended")
        voided = self.service.get(permit["id"])
        self.assertEqual(voided["status"], "revoked")
        self.assertEqual(voided["data"]["reason"], "equipment suspend: old permit voided by status change")

        # 旧许可已作废，不能凭它恢复
        with self.assertRaises(ConflictError):
            self.service.transition(self.admin, equipment["id"], "return_to_service", {})

    def test_return_to_service_blocked_by_open_remediation_and_alarm(self):
        equipment = self.equipment()
        # 挂起设备并补一张新许可
        self.service.transition(self.admin, equipment["id"], "suspend", {})
        inspection = self.service.create(
            self.admin, "inspection",
            {"equipment_id": equipment["id"], "scheduled_at": "2026-10-01T09:00:00Z", "cycle_days": 365},
        )
        self.service.transition(self.admin, inspection["id"], "pass", {"findings": "ok"})

        # 有未关闭整改：许可都发不出来
        remediation = self.service.create(
            self.admin, "remediation",
            {"equipment_id": equipment["id"], "issue": "brake wear", "owner": "Maint",
             "due_at": "2026-10-10"},
        )
        permit = self.service.create(
            self.admin, "permit",
            {"equipment_id": equipment["id"], "purpose": "return_to_service", "requested_by": "ops"},
        )
        permit = self.service.transition(self.admin, permit["id"], "request_review", {})
        with self.assertRaises(ConflictError):
            self.service.transition(self.admin, permit["id"], "grant", {})

        # 关掉整改
        self.service.transition(self.admin, remediation["id"], "submit_evidence", {"evidence": "P1"})
        self.service.transition(self.admin, remediation["id"], "verify", {})
        self.service.transition(self.admin, remediation["id"], "close", {})

        # 又有未关闭报警：许可仍然被拦
        alarm = self.service.create(
            self.dispatcher, "alarm",
            {"equipment_id": equipment["id"], "code": "TRAP-9",
             "occurred_at": "2026-10-04T09:00:00Z"},
        )
        with self.assertRaises(ConflictError):
            self.service.transition(self.admin, permit["id"], "grant", {})

        # 走完报警闭环（救援 -> 解决 -> 关闭）
        self.service.transition(self.dispatcher, alarm["id"], "dispatch", {"team": "A"})
        job = self.service.create(
            self.dispatcher, "rescue_job",
            {"alarm_id": alarm["id"], "dedupe_key": "j9", "team": "A"},
        )
        self.service.transition(self.dispatcher, job["id"], "arrive", {})
        self.service.transition(self.dispatcher, job["id"], "complete", {"outcome": "ok"})
        self.service.transition(self.dispatcher, alarm["id"], "resolve", {"resolution": "safe"})
        self.service.transition(self.dispatcher, alarm["id"], "close", {})

        permit = self.service.transition(self.admin, permit["id"], "grant", {})
        self.assertEqual(permit["status"], "granted")
        back = self.service.transition(self.admin, equipment["id"], "return_to_service", {})
        self.assertEqual(back["status"], "in_service")

    # 字段合并与状态不回退的单元行为
    def test_absorb_never_overwrites_and_status_never_moves_backwards(self):
        current = {"arrived_at": "09:10", "field_sources": {"arrived_at": "field"}}
        merged = absorb_fields(current, {"arrived_at": "10:00", "outcome": "freed"}, "central")
        self.assertEqual(merged["arrived_at"], "09:10")
        self.assertEqual(merged["outcome"], "freed")
        self.assertEqual(merged["field_sources"]["outcome"], "central")
        rank = {"dispatched": 0, "on_site": 1, "completed": 2}
        self.assertEqual(rank_status(rank, "completed", "on_site"), "completed")
        self.assertEqual(rank_status(rank, "dispatched", "on_site"), "on_site")


if __name__ == "__main__":
    unittest.main()
