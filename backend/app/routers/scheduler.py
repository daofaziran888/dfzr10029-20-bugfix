"""任务调度 时间线接口：作业时间序列、执行回执与断线恢复核对。

列表只返回调度任务本身（一个任务一行），水位记录通过详情/导出附带返回，
避免水位在列表里重复显示；详情返回的是核对后的当前水位，不再带旧残留。
"""
from __future__ import annotations

import csv
import io
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import StreamingResponse

from app.schemas import ActionPayload, ActionResult, EntryPayload, ReceiptPayload, RecoveryPayload
from app.services.scheduler import SchedulerService

router = APIRouter(prefix="/api/scheduler", tags=["任务调度"])
service = SchedulerService()
LIST_FIELDS = ["任务编号", "任务名称", "调度类型", "执行周期", "上次执行", "下次执行", "依赖关系", "任务状态"]
STATUSES = ["待执行", "执行中", "已完成", "执行失败"]
EXPORT_FIELDS = [
    "记录类型", "任务ID", "任务名称", "状态", "开始日期", "结束日期", "上次执行", "水位依据",
    "当前水位回执", "最新提交时间", "最新完成时间", "忽略回执数", "忽略回执编号",
    "忽略回执提交时间", "忽略回执完成时间", "忽略原因",
]


def csv_cell(value: Any) -> str:
    text = str(value) if value is not None else ""
    if text and text[0] in "=+-@":
        text = "'" + text
    return '"' + text.replace('"', '""') + '"'


def payload_values(payload: ReceiptPayload | None, basis: str | None = None) -> dict[str, Any]:
    if payload is None:
        return {"basis": basis} if basis else {}
    values = dict(payload.values)
    for field in ("receipt_id", "status", "result", "submitted_at", "completed_at", "watermark_basis"):
        value = getattr(payload, field, None)
        if value is not None:
            values.setdefault(field, value)
    if basis is not None:
        values["watermark_basis"] = basis
        values["basis"] = basis
    if "watermark_basis" in values and "basis" not in values:
        values["basis"] = values["watermark_basis"]
    return values


@router.get("")
def list_entries() -> dict[str, Any]:
    items = service.get_timeline()
    return {"items": items, "total": len(items), "page": 1, "size": len(items)}


@router.get("/export")
def export_entries() -> StreamingResponse:
    rows = service.export_rows()
    buf = io.StringIO(newline="")
    writer = csv.writer(buf)
    writer.writerow(EXPORT_FIELDS)
    for row in rows:
        writer.writerow([csv_cell(row.get(k, "")) for k in EXPORT_FIELDS])
    buf.seek(0)
    return StreamingResponse(iter([buf.getvalue()]), media_type="text/csv; charset=utf-8",
                             headers={"Content-Disposition": "attachment; filename=scheduler.csv"})


@router.post("/reconcile")
def reconcile_all() -> dict[str, Any]:
    """全量核对：解析既有输入，按持久化水位恢复所有任务的执行记录。"""
    return service.reconcile_all()


@router.post("/recovery")
def recover_all(payload: RecoveryPayload | None = None) -> dict[str, Any]:
    """批量断线恢复：按持久化水位核对，再处理恢复请求。"""
    return {"ok": True, "results": service.reconcile_all().get("items", [])}


@router.get("/{entry_id}/receipts")
def list_receipts(entry_id: int) -> dict[str, Any]:
    receipts, watermark = service.get_receipts(entry_id)
    if receipts is None:
        raise HTTPException(status_code=404, detail=f"调度任务 {entry_id} 不存在")
    return {"items": receipts, "watermark": watermark}


@router.get("/{entry_id}/watermarks")
def list_watermarks(entry_id: int) -> dict[str, Any]:
    watermarks, current = service.get_watermarks(entry_id)
    if watermarks is None:
        raise HTTPException(status_code=404, detail=f"调度任务 {entry_id} 不存在")
    return {"items": watermarks, "current": current, "watermark": current}


@router.post("/{entry_id}/reconcile")
def reconcile_entry(entry_id: int) -> dict[str, Any]:
    result = service.reconcile_entry(entry_id)
    if result is None:
        raise HTTPException(status_code=404, detail=f"调度任务 {entry_id} 不存在")
    return result


@router.post("/{entry_id}/receipts")
def submit_receipt(
    entry_id: int,
    payload: ReceiptPayload,
    basis: str | None = Query(default=None, description="水位依据：completed_at / submitted_at"),
) -> dict[str, Any]:
    """提交执行回执：执行结果与上次执行时间一次持久化提交。"""
    return service.submit_receipt(entry_id, payload_values(payload, basis))


@router.post("/{entry_id}/recovery")
def recover_entry(
    entry_id: int,
    payload: RecoveryPayload | None = None,
    basis: str | None = Query(default=None, description="水位依据：completed_at / submitted_at"),
) -> dict[str, Any]:
    """断线恢复：重新提交的旧回执按水位核对，不会覆盖较新的上次执行时间。"""
    return service.recover(entry_id, payload_values(payload, basis) if payload else None)


@router.post("/{entry_id}/receipt")
def submit_receipt_alias(
    entry_id: int,
    payload: ReceiptPayload,
    basis: str | None = Query(default=None, description="水位依据：completed_at / submitted_at"),
) -> dict[str, Any]:
    """执行回执接口别名，与 /receipts 行为一致。"""
    return service.submit_receipt(entry_id, payload_values(payload, basis))


@router.get("/{entry_id}")
def get_entry(entry_id: int) -> dict:
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"调度任务 {entry_id} 不存在")
    return entry


@router.post("")
def create_entry(payload: EntryPayload) -> ActionResult:
    entry_or_none, missing = service.create_entry(payload.values)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message="调度任务已登记", entry=entry_or_none)


@router.post("/{entry_id}/actions")
def run_action(entry_id: int, payload: ActionPayload) -> ActionResult:
    entry_or_none, message = service.run_action(entry_id, payload.action)
    if entry_or_none is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry_or_none)
