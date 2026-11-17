"""调度回执 / 断线恢复 / 调度水位的一致性回归测试。

不依赖 httpx/TestClient：直接驱动服务层，和路由层用的是同一个入口。
运行：backend/.venv/bin/python -m unittest discover -s backend/tests -t backend
"""
from __future__ import annotations

import unittest

from app.services.scheduler import SchedulerService
from app.store import store

TASK = 3


class SchedulerReceiptTests(unittest.TestCase):
    def setUp(self) -> None:
        store.reset()
        self.svc = SchedulerService()

    def last_execution(self, task_id: int = TASK) -> str | None:
        return self.svc.get_entry(task_id)["上次执行"]

    def test_receipt_and_last_execution_are_one_commit(self) -> None:
        result = self.svc.submit_receipt(TASK, {
            "receipt_id": "R1", "status": "已完成", "result": "ok",
            "submitted_at": "2026-11-17T10:00:00", "completed_at": "2026-11-17T10:05:00",
        })
        self.assertTrue(result["applied"])
        self.assertEqual(result["lastExecution"], "2026-11-17T10:05:00")
        self.assertEqual(self.last_execution(), "2026-11-17T10:05:00")

    def test_failed_commit_does_not_leave_time_behind(self) -> None:
        before = self.last_execution()
        with self.assertRaises(RuntimeError):
            with store.transaction("scheduler", "scheduler_receipts", "scheduler_watermarks"):
                store.find("scheduler", TASK)["上次执行"] = "2026-11-18T00:00:00"
                raise RuntimeError("断线")
        self.assertEqual(self.last_execution(), before)

    def test_recovery_resubmitting_same_result_is_idempotent(self) -> None:
        payload = {"receipt_id": "R1", "status": "已完成",
                   "submitted_at": "2026-11-17T10:00:00", "completed_at": "2026-11-17T10:05:00"}
        self.svc.submit_receipt(TASK, payload)
        again = self.svc.recover(TASK, payload)
        self.assertTrue(again["duplicate"])
        self.assertFalse(again["applied"])
        self.assertEqual(len(self.svc.get_receipts(TASK)[0]), 1)
        self.assertEqual(len(self.svc.get_watermarks(TASK)[0]), 1)
        self.assertEqual(self.last_execution(), "2026-11-17T10:05:00")

    def test_older_recovery_receipt_does_not_rewind_last_execution(self) -> None:
        self.svc.submit_receipt(TASK, {
            "receipt_id": "NEW", "status": "已完成",
            "submitted_at": "2026-11-17T12:00:00", "completed_at": "2026-11-17T12:06:00",
        })
        stale = self.svc.recover(TASK, {
            "receipt_id": "OLD", "status": "已完成",
            "submitted_at": "2026-11-17T09:00:00", "completed_at": "2026-11-17T09:04:00",
        })
        self.assertFalse(stale["applied"])
        self.assertEqual(stale["lastExecution"], "2026-11-17T12:06:00")
        self.assertEqual(self.last_execution(), "2026-11-17T12:06:00")
        outcomes = {r["receiptId"]: r["outcome"] for r in self.svc.get_receipts(TASK)[0]}
        self.assertEqual(outcomes["NEW"], "applied")
        self.assertEqual(outcomes["OLD"], "ignored")

    def test_watermark_basis_selects_different_but_valid_results(self) -> None:
        # 完成时间较早、提交时间较新：按不同水位依据会得到不同的执行记录。
        self.svc.submit_receipt(TASK, {
            "receipt_id": "A", "submitted_at": "2026-11-17T12:10:00",
            "completed_at": "2026-11-17T12:00:00", "basis": "completed_at",
        })
        stale = self.svc.submit_receipt(TASK, {
            "receipt_id": "B", "submitted_at": "2026-11-17T12:20:00",
            "completed_at": "2026-11-17T11:00:00",
        })
        self.assertFalse(stale["applied"])
        self.assertEqual(stale["watermark"]["basis"], "completed_at")
        self.assertEqual(stale["lastExecution"], "2026-11-17T12:00:00")

        store.reset()
        svc = SchedulerService()
        svc.submit_receipt(TASK, {
            "receipt_id": "A", "submitted_at": "2026-11-17T12:10:00",
            "completed_at": "2026-11-17T12:00:00", "basis": "submitted_at",
        })
        newer = svc.submit_receipt(TASK, {
            "receipt_id": "B", "submitted_at": "2026-11-17T12:20:00",
            "completed_at": "2026-11-17T11:00:00",
        })
        self.assertTrue(newer["applied"])
        self.assertEqual(newer["watermark"]["basis"], "submitted_at")
        self.assertEqual(newer["lastExecution"], "2026-11-17T12:20:00")

    def test_duplicate_watermark_rows_are_collapsed(self) -> None:
        row = store.find("scheduler", 1)
        row["调度水位记录"] = [{"receipt_id": "OLD", "上次执行": "2026-11-16T08:00:00"}] * 2
        row["上次执行"] = "2026-11-16T08:00:00"
        table = store.aux_rows("scheduler_watermarks")
        for index in range(3):
            table.append({
                "id": index + 1, "task_id": 1, "receipt_id": "OLD",
                "last_execution": "2026-11-16T08:00:00",
                "completed_at": "2026-11-16T08:00:00", "basis": "completed_at",
            })

        detail = self.svc.get_entry(1)
        self.assertNotIn("调度水位记录", detail)
        self.assertEqual(len(detail["watermarks"]), 1)
        self.assertEqual(len(self.svc.get_watermarks(1)[0]), 1)
        # 列表不重复出现水位行。
        ids = [item["id"] for item in self.svc.get_timeline()]
        self.assertEqual(len(ids), len(set(ids)))
        self.assertEqual(self.svc.get_timeline()[0]["lastExecution"], "2026-11-16T08:00:00")

    def test_reconcile_repairs_rewound_last_execution(self) -> None:
        row = store.find("scheduler", TASK)
        row["上次执行"] = "2026-11-17T09:04:00"  # 被旧恢复写坏的时间
        store.aux_rows("scheduler_watermarks").append({
            "id": 1, "task_id": TASK, "receipt_id": "NEW",
            "last_execution": "2026-11-17T12:06:00",
            "submitted_at": "2026-11-17T12:00:00",
            "completed_at": "2026-11-17T12:06:00", "basis": "completed_at",
        })
        store.aux_rows("scheduler_receipts").append({
            "id": 1, "task_id": TASK, "receipt_id": "NEW",
            "signature": "3|2026-11-17T12:00:00|2026-11-17T12:06:00|已完成|",
            "submitted_at": "2026-11-17T12:00:00",
            "completed_at": "2026-11-17T12:06:00",
            "status": "已完成", "result": "", "outcome": "pending",
        })

        result = self.svc.reconcile_entry(TASK)
        self.assertTrue(result["corrected"])
        self.assertEqual(result["entry"]["上次执行"], "2026-11-17T12:06:00")
        self.assertEqual(result["executions"][0]["outcome"], "duplicate")
        # 再核对一次不会多出确认结果。
        self.assertFalse(self.svc.reconcile_entry(TASK)["corrected"])
        self.assertEqual(len(self.svc.get_receipts(TASK)[0]), 1)
        self.assertEqual(len(self.svc.get_watermarks(TASK)[0]), 1)

    def test_recovery_without_timestamps_does_not_advance_watermark(self) -> None:
        self.svc.submit_receipt(TASK, {
            "receipt_id": "R1", "submitted_at": "2026-11-17T10:00:00",
            "completed_at": "2026-11-17T10:05:00",
        })
        result = self.svc.recover(TASK, {"receipt_id": "R1-NO-TIME", "status": "已完成"})
        self.assertFalse(result["ok"])
        self.assertEqual(self.last_execution(), "2026-11-17T10:05:00")
        self.assertEqual(len(self.svc.get_receipts(TASK)[0]), 1)

    def test_export_keeps_ignored_receipt_and_current_watermark_basis(self) -> None:
        self.svc.submit_receipt(TASK, {
            "receipt_id": "NEW", "submitted_at": "2026-11-17T12:00:00",
            "completed_at": "2026-11-17T12:06:00",
        })
        self.svc.recover(TASK, {
            "receipt_id": "OLD", "submitted_at": "2026-11-17T08:00:00",
            "completed_at": "2026-11-17T08:03:00",
        })
        rows = [row for row in self.svc.export_rows() if row["任务ID"] == TASK]
        task_rows = [row for row in rows if row["记录类型"] == "调度任务"]
        ignored_rows = [row for row in rows if row["记录类型"] == "已忽略旧回执"]
        self.assertEqual(len(task_rows), 1)
        self.assertEqual(task_rows[0]["上次执行"], "2026-11-17T12:06:00")
        self.assertEqual(task_rows[0]["水位依据"], "最新完成时间")
        self.assertEqual(task_rows[0]["当前水位回执"], "NEW")
        self.assertEqual({row["忽略回执编号"] for row in ignored_rows}, {"OLD"})


if __name__ == "__main__":
    unittest.main()
