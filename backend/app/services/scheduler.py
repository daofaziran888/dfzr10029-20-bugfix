"""任务调度服务：作业时间线、执行回执与调度水位的一致性处理。

这里的核心约束：

* **一次提交**：执行结果（回执）与任务的「上次执行」在同一次
  ``store.transaction`` 里写入，避免断线恢复时只写了一半时间、结果没落库。
* **水位只进不退**：调度水位（watermark）记录任务当前已确认的最新执行时间；
  任何恢复请求重新提交旧回执时只能被记为「已忽略」，不能把「上次执行」改回去。
* **幂等**：同一条回执（回执编号相同，或时间/结果完全一致）重复提交不会产生
  第二份确认结果，也不会新增第二条水位记录。
* **读取即核对**：读取时间线/详情时先解析既有输入（历史回执、持久化水位、
  旧版本留在任务行里的水位残留），去掉重复水位，再把纠正结果写回。
"""
from __future__ import annotations

import hashlib
import json
from datetime import date, datetime, timedelta, timezone
from typing import Any, Iterable

from app.store import store

MODULE = "scheduler"
REQUIRED_FIELDS = ["任务编号", "任务名称", "调度类型"]
STATUS_ORDER = ["待执行", "执行中", "已完成", "执行失败"]
ACTION_RULES = {"立即执行": "执行中", "暂停任务": "待执行", "标记失败": "执行失败"}
NEGATIVE_ACTIONS = []

# 配套表：不参与概览页的模块统计，但和 scheduler 主表一起提交。
RECEIPT_TABLE = "scheduler_receipts"
WATERMARK_TABLE = "scheduler_watermarks"

DEFAULT_WATERMARK_BASIS = "completed_at"
BASIS_ALIASES = {
    "completed_at": "completed_at",
    "completed": "completed_at",
    "完成时间": "completed_at",
    "最新完成时间": "completed_at",
    "submitted_at": "submitted_at",
    "submitted": "submitted_at",
    "提交时间": "submitted_at",
    "最新提交时间": "submitted_at",
}
BASIS_LABELS = {
    "completed_at": "最新完成时间",
    "submitted_at": "最新提交时间",
}

# 旧版本把水位直接塞在任务行里，读取时要消费掉，避免详情继续显示旧残留。
LEGACY_WATERMARK_KEYS = ("调度水位记录", "水位记录", "watermark", "watermarks", "scheduler_watermark")
LEGACY_RESIDUE_KEYS = ("执行回执", "receipts", "lastReceipt", "last_receipt", "watermark_record")

_TIME_FORMATS = (
    "%Y-%m-%dT%H:%M:%S.%f",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%d %H:%M:%S.%f",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M",
    "%Y/%m/%d %H:%M:%S",
    "%Y/%m/%d %H:%M",
    "%Y%m%d%H%M%S",
)


def parse_time(value: Any) -> datetime | None:
    """把各种写法的时间解析成不带时区的 UTC 时间，解析不了就返回 None。"""
    if value is None:
        return None
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, date):
        parsed = datetime(value.year, value.month, value.day)
    else:
        text = str(value).strip()
        if not text:
            return None
        text = text.replace("Z", "+00:00")
        parsed = None
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError:
            for fmt in _TIME_FORMATS:
                try:
                    parsed = datetime.strptime(text, fmt)
                    break
                except ValueError:
                    continue
        if parsed is None:
            return None
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return parsed.replace(microsecond=0)


def iso_time(value: datetime | None) -> str | None:
    return value.strftime("%Y-%m-%dT%H:%M:%S") if value else None


def _pick(values: dict[str, Any], *names: str) -> Any:
    for name in names:
        if name in values and values[name] not in (None, ""):
            return values[name]
    return None


def _normalize_basis(value: Any) -> str | None:
    if value is None:
        return None
    return BASIS_ALIASES.get(str(value).strip())


def _int_id(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


class SchedulerService:
    # ------------------------------------------------------------------ 读取
    def get_timeline(self) -> list[dict[str, Any]]:
        """列表：一个任务只出现一次，水位记录不会作为独立行重复显示。"""
        today = date.today()
        items: list[dict] = []
        seen_ids: set[int] = set()
        seen_numbers: set[str] = set()
        for i, row in enumerate(list(store.rows(MODULE))):
            task_id = _int_id(row.get("id", i + 1), i + 1)
            number = str(row.get("任务编号") or "").strip()
            # 历史数据里同一条任务被水位记录"带"出多行时按 id / 任务编号折叠。
            if task_id in seen_ids or (number and number in seen_numbers):
                continue
            seen_ids.add(task_id)
            if number:
                seen_numbers.add(number)

            state = self._load_state(row, persist=True)
            current = state["watermark"]
            start = (today - timedelta(days=7 - i * 2)).isoformat()
            end = (today + timedelta(days=3 - i)).isoformat()
            last_execution = state["last_execution_display"]
            ignored = state["ignored_receipts"]
            log_lines = [f"[{start}] 作业启动", f"[{start}] 正在处理..."]
            if current and current.get("last_execution"):
                moment = iso_time(current["last_execution"])
                basis_label = BASIS_LABELS.get(current.get("basis", DEFAULT_WATERMARK_BASIS), current.get("basis"))
                log_lines.append(f"[{moment}] 最近一次执行已确认（水位依据：{basis_label}）")
            else:
                log_lines.append(f"[{end}] 任务{'完成' if row.get('status') == STATUS_ORDER[2] else '进行中'}")
            if ignored:
                log_lines.append(f"已忽略 {len(ignored)} 条旧回执：{'、'.join(str(r.get('receipt_id')) for r in ignored)}")
            items.append({
                "id": task_id,
                "name": str(row.get("任务名称") or row.get(list(row.keys())[1] if len(row) > 1 else "name", "")),
                "start": start,
                "end": end,
                "status": str(row.get("status", STATUS_ORDER[i % len(STATUS_ORDER)])),
                "logs": "\n".join(log_lines),
                "lastExecution": last_execution,
                "上次执行": last_execution,
                "watermarkBasis": current.get("basis") if current else None,
                "水位依据": BASIS_LABELS.get(current.get("basis"), current.get("basis")) if current else None,
                "watermarkReceipt": current.get("receipt_id") if current else None,
                "latestSubmittedAt": iso_time(current.get("submitted_at")) if current else None,
                "latestCompletedAt": iso_time(current.get("completed_at")) if current else None,
                "receiptCount": len(state["receipts"]),
                "ignoredReceiptCount": len(ignored),
                "ignoredReceipts": [self._receipt_view(r) for r in ignored],
            })
        return items

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None
        return self.get_entry_detail(entry_id)

    def get_entry_detail(self, entry_id: int) -> dict[str, Any] | None:
        """详情：返回当前水位与回执核对结果，不再带旧水位残留。"""
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None
        state = self._load_state(entry, persist=True)
        detail = dict(entry)
        detail["上次执行"] = state["last_execution_display"]
        current = state["watermark"]
        detail["watermark"] = self._watermark_view(state["watermark"])
        detail["调度水位"] = detail["watermark"]
        detail["watermarkBasis"] = state["watermark"].get("basis") if state["watermark"] else None
        detail["水位依据"] = (
            BASIS_LABELS.get(state["watermark"].get("basis"), state["watermark"].get("basis"))
            if state["watermark"] else None
        )
        detail["receipts"] = [self._receipt_view(r) for r in state["receipts"]]
        detail["ignoredReceipts"] = [self._receipt_view(r) for r in state["ignored_receipts"]]
        detail["executions"] = detail["receipts"]
        detail["watermarks"] = [self._watermark_view(w) for w in state["watermarks"]]
        return detail

    def get_receipts(self, entry_id: int) -> tuple[list[dict[str, Any]] | None, dict[str, Any] | None]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, None
        state = self._load_state(entry, persist=True)
        return [self._receipt_view(r) for r in state["receipts"]], self._watermark_view(state["watermark"])

    def get_watermarks(self, entry_id: int) -> tuple[list[dict[str, Any]] | None, dict[str, Any] | None]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, None
        state = self._load_state(entry, persist=True)
        return [self._watermark_view(w) for w in state["watermarks"]], self._watermark_view(state["watermark"])

    # ------------------------------------------------------------------ 写入
    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [f for f in REQUIRED_FIELDS if not str(values.get(f) or "").strip()]
        if missing:
            return None, missing
        with store.transaction(MODULE, RECEIPT_TABLE, WATERMARK_TABLE):
            rows = store.rows(MODULE)
            entry = {"id": max((int(r.get("id", 0)) for r in rows), default=0) + 1}
            entry.update({f: values.get(f) for f in REQUIRED_FIELDS})
            entry["status"] = STATUS_ORDER[0]
            entry["pending"] = True
            entry["abnormal"] = False
            rows.append(entry)
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"调度任务 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于任务调度可执行范围"
        target = ACTION_RULES[action]
        with store.transaction(MODULE):
            entry["status"] = target
            entry["pending"] = target != STATUS_ORDER[-1]
            entry["abnormal"] = action in NEGATIVE_ACTIONS
        return entry, f"调度任务已{action}"

    def submit_receipt(
        self,
        entry_id: int,
        values: dict[str, Any],
        *,
        recovery: bool = False,
    ) -> dict[str, Any]:
        """提交一条执行回执；恢复请求走同一个入口，保证幂等与水位单调。"""
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return {"ok": False, "applied": False, "message": f"调度任务 {entry_id} 不存在或已归档"}

        explicit_basis = self._resolve_basis(values, entry, default="")
        try:
            # 回执编号在事务内分配，避免并发提交拿到同一个序号。
            # 恢复请求必须自带提交/完成时间：缺时间就按"现在"处理会把旧结果
            # 误判成最新执行，反而把水位推高，所以这里不能兜底成当前时间。
            normalized = self._normalize_receipt(entry_id, values, default_now=not recovery)
        except ValueError as exc:
            return {"ok": False, "applied": False, "duplicate": False, "recovery": recovery,
                    "message": str(exc)}
        with store.transaction(MODULE, RECEIPT_TABLE, WATERMARK_TABLE):
            # 先清理重复/旧残留，再追加新水位；否则重放会重复判定既有回执。
            state = self._load_state(entry, persist=True)
            current = state["watermark"]
            # 水位依据按任务粘住：同一条任务后续回执沿用已选口径，避免两次提交
            # 在「最新提交时间」和「最新完成时间」之间来回换算。
            basis = explicit_basis or (current.get("basis") if current else "") or DEFAULT_WATERMARK_BASIS
            receipt = normalized
            duplicates = self._find_duplicate(state["receipts"], receipt)
            if duplicates:
                # 重复提交：沿用第一次确认的结果，不再产生第二份确认/第二条水位。
                first = duplicates[0]
                first["duplicate_submissions"] = int(first.get("duplicate_submissions", 0)) + 1
                first["last_repeated_at"] = iso_time(datetime.now())
                return self._result(
                    entry, state, first, applied=False, duplicate=True, recovery=recovery,
                    message="该执行回执已确认过，本次重复提交已忽略",
                )

            outcome, reason, candidate = self._decide(
                state["watermark"], receipt, basis, state["last_execution_floor"]
            )
            receipt["outcome"] = outcome
            receipt["reason"] = reason
            receipt["basis"] = basis
            receipt["recovery"] = recovery
            receipt["duplicate_submissions"] = 0
            store.aux_rows(RECEIPT_TABLE).append(receipt)
            state["receipts"].append(receipt)
            if outcome == "ignored":
                state["ignored_receipts"].append(receipt)

            if outcome == "applied":
                new_watermark = self._apply_watermark(state, receipt, candidate, basis)
                state["watermark"] = new_watermark
                state["watermarks"] = [new_watermark]
                self._persist_state(_int_id(entry.get("id")), new_watermark, 0)
            # 一次提交内同时落执行结果、水位和任务上的「上次执行」。
            self._persist_task_last_execution(entry, state)

            return self._result(
                entry, state, receipt, applied=outcome == "applied", duplicate=False,
                recovery=recovery, message=reason,
            )

    def recover(self, entry_id: int, values: dict[str, Any] | None = None) -> dict[str, Any]:
        """断线恢复：先按持久化水位核对既有回执，再处理这次恢复请求。

        没有携带新回执（只有重连信号）时，只做核对，不凭空写一条执行结果。
        """
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return {"ok": False, "applied": False, "message": f"调度任务 {entry_id} 不存在或已归档"}
        corrected = self.reconcile_entry(entry_id)
        if not values or not any(values.get(key) not in (None, "") for key in (
            "receipt_id", "status", "result", "submitted_at", "completed_at",
        )):
            result: dict[str, Any] = dict(corrected or {})
            result.update({
                "ok": True,
                "applied": False,
                "duplicate": False,
                "recovery": True,
                "lastExecution": entry.get("上次执行"),
                "上次执行": entry.get("上次执行"),
                "message": "断线恢复已按持久化水位核对，本次未提交新的执行结果",
            })
            return result
        result = self.submit_receipt(entry_id, values, recovery=True)
        result["corrected"] = bool(corrected.get("corrected")) if corrected else False
        return result

    def reconcile_entry(self, entry_id: int) -> dict[str, Any] | None:
        """按既有次序重放回执，用持久化水位纠正任务的执行记录。

        已经确认过（applied / ignored / duplicate）的回执保持原判，不重复判定；
        只对从未处理的回执按次序补判定，并把任务的「上次执行」校正到水位时间，
        这样旧版本写坏的时间（倒退）会被改回较新的水位，而不是再被读旧回执覆盖。
        """
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None
        raw_before = entry.get("上次执行")
        with store.transaction(MODULE, RECEIPT_TABLE, WATERMARK_TABLE):
            state = self._load_state(entry, persist=True)
            applied: list[dict[str, Any]] = []
            watermark = state["watermark"]
            floor = state["last_execution_floor"]

            seen_signatures: set[str] = set()
            ordered = sorted(state["receipts"], key=lambda r: (_int_id(r.get("id")), _int_id(r.get("seq"))))
            for receipt in ordered:
                signature = self._signature(receipt)
                if signature in seen_signatures:
                    receipt["outcome"] = "duplicate"
                    receipt["reason"] = "同一执行结果重复写入，已忽略"
                    continue
                seen_signatures.add(signature)

                outcome = receipt.get("outcome")
                if outcome in ("applied", "ignored", "duplicate"):
                    if outcome == "applied":
                        # 水位落库失败时按已确认的回执补回来；只前进不后退。
                        completed = parse_time(receipt.get("completed_at"))
                        if completed and (watermark is None or completed > (watermark.get("last_execution") or datetime.min)):
                            candidate = {
                                "last_execution": max(completed, floor),
                                "submitted_at": parse_time(receipt.get("submitted_at")) or completed,
                                "completed_at": completed,
                            }
                            watermark = self._apply_watermark(
                                {"watermark": watermark, "watermarks": []},
                                receipt, candidate, self._resolve_basis(receipt, entry),
                            )
                            floor = max(floor, completed)
                            applied.append(receipt)
                    continue

                # 已反映在当前水位里的回执就是重复提交，不再产生第二份确认结果。
                if watermark is not None and (
                    (receipt.get("receipt_id") and receipt.get("receipt_id") == watermark.get("receipt_id"))
                    or (parse_time(receipt.get("completed_at")) == watermark.get("completed_at")
                        and parse_time(receipt.get("submitted_at")) == watermark.get("submitted_at"))
                ):
                    receipt["outcome"] = "duplicate"
                    receipt["reason"] = "该执行结果已反映在当前水位中，按重复提交忽略"
                    continue

                outcome, reason, candidate = self._decide(
                    watermark, receipt, self._resolve_basis(receipt, entry), floor
                )
                receipt["outcome"] = outcome
                receipt["reason"] = reason
                if outcome == "applied":
                    watermark = self._apply_watermark(
                        {"watermark": watermark, "last_execution_floor": floor, "watermarks": []},
                        receipt, candidate, self._resolve_basis(receipt, entry),
                    )
                    floor = max(floor, parse_time(receipt.get("completed_at")) or floor)
                    applied.append(receipt)

            state["watermark"] = watermark
            state["last_execution_floor"] = floor
            state["watermarks"] = [watermark] if watermark else []
            self._persist_state(entry_id, watermark, 0)
            self._persist_task_last_execution(entry, state)
            after = entry.get("上次执行")
            corrected = raw_before != after
            return {
                "ok": True,
                "corrected": corrected,
                "entry": self.get_entry_detail(entry_id),
                "watermark": self._watermark_view(watermark),
                "executions": [self._receipt_view(r) for r in state["receipts"]],
                "appliedReceipts": [self._receipt_view(r) for r in applied],
                "ignoredReceipts": [self._receipt_view(r) for r in state["ignored_receipts"]],
                "message": "调度执行记录已按持久化水位核对完成",
            }

    def reconcile_all(self) -> dict[str, Any]:
        results = [self.reconcile_entry(_int_id(row.get("id"))) for row in list(store.rows(MODULE))]
        results = [r for r in results if r]
        return {
            "ok": True,
            "corrected": any(r.get("corrected") for r in results),
            "items": results,
            "message": f"已核对 {len(results)} 个调度任务",
        }

    # ------------------------------------------------------------- 既有输入解析
    def _load_state(self, entry: dict[str, Any], *, persist: bool) -> dict[str, Any]:
        """解析任务行、回执表与持久化水位，返回去重后的当前水位。"""
        with store.lock:
            return self._load_state_locked(entry, persist=persist)

    def _load_state_locked(self, entry: dict[str, Any], *, persist: bool) -> dict[str, Any]:
        """解析任务行、回执表与持久化水位，返回去重后的当前水位。

        水位表对每个任务只保留一条「当前水位」；历史水位由回执与当前水位共同表达，
        因此列表、详情、导出都不会重复出现同一条水位记录。
        """
        task_id = _int_id(entry.get("id"))
        receipts = [r for r in store.aux_rows(RECEIPT_TABLE) if _int_id(r.get("task_id")) == task_id]

        watermarks: list[dict[str, Any]] = []
        for raw in store.aux_rows(WATERMARK_TABLE):
            if _int_id(raw.get("task_id")) != task_id:
                continue
            record = self._normalize_watermark(raw, task_id)
            if record is not None:
                watermarks.append(record)

        # 旧版本残留：任务行里内嵌的水位记录同样作为核对输入。
        for legacy in self._legacy_watermarks(entry, task_id):
            watermarks.append(legacy)

        watermarks, duplicates_removed = self._dedupe_watermarks(watermarks)

        raw_last = entry.get("上次执行")
        parsed_last = parse_time(raw_last)
        basis = watermarks[-1].get("basis") if watermarks else DEFAULT_WATERMARK_BASIS

        # 当前水位取「已持久化水位」与「任务行上次执行」中较新的那个：只前进不后退。
        candidates = [w for w in watermarks if w.get("last_execution")]
        if parsed_last is not None:
            candidates.append(self._make_watermark(
                task_id=task_id,
                receipt_id=self._latest_receipt_id(watermarks, parsed_last),
                last_execution=parsed_last,
                submitted_at=parsed_last,
                completed_at=parsed_last,
                basis=basis,
                source="legacy_task",
            ))
        current = max(
            candidates,
            key=lambda w: (w.get("last_execution"), w.get("completed_at")),
            default=None,
        )

        if current is not None:
            # 任务行上的旧残留清理掉，详情只展示合并后的当前水位。
            for key in LEGACY_WATERMARK_KEYS + LEGACY_RESIDUE_KEYS:
                entry.pop(key, None)

        last_execution_floor = current.get("last_execution") if current else None
        if parsed_last is not None and (last_execution_floor is None or parsed_last > last_execution_floor):
            last_execution_floor = parsed_last

        if persist:
            self._persist_state(task_id, current, duplicates_removed)
            if current is not None and current.get("last_execution"):
                if parsed_last is None or parsed_last != current["last_execution"]:
                    entry["上次执行"] = iso_time(current["last_execution"])

        return {
            "receipts": receipts,
            "watermarks": [current] if current else [],
            "watermark": current,
            "ignored_receipts": [r for r in receipts if r.get("outcome") == "ignored"],
            "last_execution_floor": last_execution_floor or datetime.min,
            "last_execution_display": iso_time(last_execution_floor) if last_execution_floor
            else (raw_last if raw_last not in (None, "") else None),
            "duplicates_removed": duplicates_removed,
        }

    def _latest_receipt_id(self, watermarks: list[dict[str, Any]], moment: datetime) -> str | None:
        for record in reversed(watermarks):
            if record.get("last_execution") == moment and record.get("receipt_id"):
                return record["receipt_id"]
        return None

    def _persist_state(self, task_id: int, current: dict[str, Any] | None, duplicates_removed: int) -> None:
        """把一个任务的水位落库：只保留一条当前水位，重复记录直接清掉。"""
        keep: list[dict[str, Any]] = []
        if current is not None:
            keep.append({
                "id": _int_id(current.get("id")) or self._next_watermark_id(),
                "task_id": task_id,
                "receipt_id": current.get("receipt_id"),
                "last_execution": iso_time(current.get("last_execution")),
                "submitted_at": iso_time(current.get("submitted_at")),
                "completed_at": iso_time(current.get("completed_at")),
                "basis": current.get("basis") or DEFAULT_WATERMARK_BASIS,
                "applied_at": iso_time(current.get("applied_at") or datetime.now()),
                "superseded": False,
                "source": current.get("source") or "persisted",
            })
        others = [w for w in store.aux_rows(WATERMARK_TABLE) if _int_id(w.get("task_id")) != task_id]
        table = store.aux_rows(WATERMARK_TABLE)
        table[:] = others + keep
        if duplicates_removed:
            store.aux_rows("scheduler_watermark_dedupe_log").append(
                {"task_id": task_id, "removed": duplicates_removed}
            )

    def _next_watermark_id(self) -> int:
        return max((_int_id(w.get("id")) for w in store.aux_rows(WATERMARK_TABLE)), default=0) + 1

    def _legacy_watermarks(self, entry: dict[str, Any], task_id: int) -> list[dict[str, Any]]:
        raw_values: list[Any] = []
        for key in LEGACY_WATERMARK_KEYS:
            if key in entry:
                raw_values.append(entry.get(key))
        records: list[dict[str, Any]] = []
        for raw in raw_values:
            for item in self._iter_legacy_records(raw):
                record = self._watermark_from_legacy(item, task_id)
                if record is not None:
                    record["legacy_only"] = True
                    records.append(record)
        return records

    def _iter_legacy_records(self, raw: Any) -> Iterable[Any]:
        if raw is None:
            return
        if isinstance(raw, (list, tuple)):
            for item in raw:
                yield item
            return
        if isinstance(raw, dict):
            # 有时旧数据是 {"records": [...]} 或单条水位字典。
            for key in ("records", "items", "history", "水位记录"):
                if key in raw and isinstance(raw[key], (list, tuple)):
                    for item in raw[key]:
                        yield item
                    return
            yield raw
            return
        if isinstance(raw, str):
            text = raw.strip()
            if not text:
                return
            try:
                decoded = json.loads(text)
            except (TypeError, ValueError):
                yield text
                return
            yield from self._iter_legacy_records(decoded)

    def _watermark_from_legacy(self, raw: Any, task_id: int) -> dict[str, Any] | None:
        if isinstance(raw, dict):
            last = parse_time(_pick(raw, "last_execution", "上次执行", "watermark", "水位", "执行时间"))
            submitted = parse_time(_pick(raw, "submitted_at", "提交时间", "latest_submitted_at"))
            completed = parse_time(_pick(raw, "completed_at", "完成时间", "latest_completed_at"))
            if last is None and completed is None and submitted is None:
                return None
            last = last or completed or submitted
            submitted = submitted or last
            completed = completed or last
            basis = _normalize_basis(_pick(raw, "basis", "水位依据")) or DEFAULT_WATERMARK_BASIS
            return self._make_watermark(
                task_id=task_id,
                receipt_id=_pick(raw, "receipt_id", "回执编号", "current_receipt"),
                last_execution=max(filter(None, [last, completed, submitted])),
                submitted_at=submitted,
                completed_at=completed,
                basis=basis,
                source="legacy_embedded",
            )
        parsed = parse_time(raw)
        if parsed is None:
            return None
        return self._make_watermark(
            task_id=task_id,
            receipt_id=None,
            last_execution=parsed,
            submitted_at=parsed,
            completed_at=parsed,
            basis=DEFAULT_WATERMARK_BASIS,
            source="legacy_embedded",
        )

    def _normalize_watermark(self, raw: dict[str, Any], task_id: int) -> dict[str, Any] | None:
        last = parse_time(_pick(raw, "last_execution", "上次执行", "watermark_time"))
        submitted = parse_time(_pick(raw, "submitted_at", "最新提交时间"))
        completed = parse_time(_pick(raw, "completed_at", "最新完成时间"))
        if last is None and completed is None and submitted is None:
            return None
        basis = _normalize_basis(raw.get("basis")) or DEFAULT_WATERMARK_BASIS
        # 水位时间按已存口径还原，不能简单取三者最大值：完成时间与提交时间
        # 分处两侧时，"按完成时间"和"按提交时间"会得到不同的执行记录。
        if last is None:
            last = (submitted if basis == "submitted_at" else completed) or completed or submitted
        submitted = submitted or last
        completed = completed or last
        return {
            "id": _int_id(raw.get("id")),
            "task_id": task_id,
            "receipt_id": raw.get("receipt_id"),
            "last_execution": last,
            "submitted_at": submitted,
            "completed_at": completed,
            "basis": basis,
            "applied_at": parse_time(raw.get("applied_at")),
            "superseded": bool(raw.get("superseded", False)),
            "source": "persisted",
        }

    def _dedupe_watermarks(self, records: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], int]:
        """同一条水位（同回执或同时间）只保留一条，按时间排序。"""
        ordered = sorted(
            records,
            key=lambda r: (
                r.get("last_execution") or datetime.min,
                r.get("completed_at") or datetime.min,
                r.get("submitted_at") or datetime.min,
                _int_id(r.get("id")),
            ),
        )
        deduped: list[dict[str, Any]] = []
        seen: set[tuple] = set()
        removed = 0
        for record in ordered:
            key = (
                record.get("receipt_id") or "",
                record.get("last_execution"),
                record.get("completed_at"),
            )
            if key in seen:
                removed += 1
                continue
            seen.add(key)
            record["id"] = _int_id(record.get("id")) or (max((_int_id(r.get("id")) for r in deduped), default=0) + 1)
            deduped.append(record)
        return deduped, removed

    def _make_watermark(
        self,
        *,
        task_id: int,
        receipt_id: str | None,
        last_execution: datetime,
        submitted_at: datetime | None,
        completed_at: datetime | None,
        basis: str,
        source: str,
    ) -> dict[str, Any]:
        return {
            "id": 0,
            "task_id": task_id,
            "receipt_id": receipt_id,
            "last_execution": last_execution,
            "submitted_at": submitted_at or last_execution,
            "completed_at": completed_at or last_execution,
            "basis": basis,
            "applied_at": datetime.now().replace(microsecond=0),
            "superseded": False,
            "source": source,
        }

    # ------------------------------------------------------------- 回执与水位判断
    def _normalize_receipt(
        self,
        task_id: int,
        values: dict[str, Any],
        *,
        default_now: bool = True,
    ) -> dict[str, Any]:
        flattened: dict[str, Any] = {}
        nested = values.get("values")
        if isinstance(nested, dict):
            flattened.update(nested)
        flattened.update({k: v for k, v in values.items() if k != "values"})

        now = datetime.now().replace(microsecond=0)
        submitted = parse_time(_pick(flattened, "submitted_at", "提交时间", "received_at", "送达时间", "report_at"))
        completed = parse_time(_pick(flattened, "completed_at", "完成时间", "finished_at", "执行完成时间", "executed_at"))
        if submitted is None and completed is None:
            if not default_now:
                raise ValueError("执行回执缺少提交时间/完成时间")
            submitted = completed = now
        submitted = submitted or completed
        completed = completed or submitted
        assert submitted is not None and completed is not None

        status = str(_pick(flattened, "status", "执行状态", "结果状态") or "已完成")
        result = _pick(flattened, "result", "执行结果", "输出结果", "remark", "备注")
        result = "" if result is None else str(result)
        receipt_id = _pick(flattened, "receipt_id", "回执编号", "execution_id", "执行编号")
        receipt_id = str(receipt_id).strip() if receipt_id not in (None, "") else ""

        signature = self._signature({
            "task_id": task_id,
            "submitted_at": submitted,
            "completed_at": completed,
            "status": status,
            "result": result,
        })
        if not receipt_id:
            # 没有显式回执编号时按内容生成，重复提交同一结果仍会命中幂等。
            receipt_id = "RCPT-" + hashlib.sha1(signature.encode("utf-8")).hexdigest()[:12].upper()

        rows = store.aux_rows(RECEIPT_TABLE)
        return {
            "id": max((_int_id(r.get("id")) for r in rows), default=0) + 1,
            "task_id": task_id,
            "receipt_id": receipt_id,
            "signature": signature,
            "submitted_at": iso_time(submitted),
            "completed_at": iso_time(completed),
            "status": status,
            "result": result,
            "outcome": "pending",
            "reason": "",
            "basis": DEFAULT_WATERMARK_BASIS,
            "recovery": False,
            "received_at": iso_time(now),
            "duplicate_submissions": 0,
        }

    def _signature(self, receipt: dict[str, Any]) -> str:
        parts = [
            str(receipt.get("task_id") or ""),
            self._time_text(receipt.get("submitted_at")),
            self._time_text(receipt.get("completed_at")),
            str(receipt.get("status") or ""),
            str(receipt.get("result") or ""),
        ]
        return "|".join(parts)

    def _time_text(self, value: Any) -> str:
        parsed = parse_time(value)
        return iso_time(parsed) if parsed else str(value or "")

    def _find_duplicate(self, receipts: list[dict[str, Any]], incoming: dict[str, Any]) -> list[dict[str, Any]]:
        matches: list[dict[str, Any]] = []
        incoming_signature = incoming.get("signature") or self._signature(incoming)
        for receipt in receipts:
            if receipt.get("receipt_id") and receipt.get("receipt_id") == incoming.get("receipt_id"):
                matches.append(receipt)
            elif (receipt.get("signature") or self._signature(receipt)) == incoming_signature:
                matches.append(receipt)
        return matches

    def _resolve_basis(self, values: dict[str, Any], entry: dict[str, Any], *, default: str | None = DEFAULT_WATERMARK_BASIS) -> str:
        candidates = []
        nested = values.get("values")
        if isinstance(nested, dict):
            candidates.append(_pick(nested, "watermark_basis", "水位依据"))
        candidates.append(_pick(values, "watermark_basis", "水位依据", "basis"))
        candidates.append(_pick(entry, "水位依据", "watermark_basis", "watermarkBasis"))
        for candidate in candidates:
            basis = _normalize_basis(candidate)
            if basis:
                return basis
        return default or ""

    def _decide(
        self,
        current: dict[str, Any] | None,
        receipt: dict[str, Any],
        basis: str,
        floor: datetime,
    ) -> tuple[str, str, dict[str, Any]]:
        """判断回执是应用还是忽略；忽略时绝不能把上次执行时间改小。"""
        completed = parse_time(receipt.get("completed_at"))
        submitted = parse_time(receipt.get("submitted_at"))
        if completed is None or submitted is None:
            return "ignored", "回执缺少可解析的提交/完成时间，未写入水位", {}

        if current is None:
            last_execution = max(completed, floor)
            return "applied", "首次执行回执，已建立调度水位", {
                "last_execution": last_execution,
                "submitted_at": submitted,
                "completed_at": completed,
            }

        cur_completed = current.get("completed_at") or current.get("last_execution") or datetime.min
        cur_submitted = current.get("submitted_at") or cur_completed
        cur_last = current.get("last_execution") or cur_completed

        if basis == "submitted_at":
            primary_new, primary_old = submitted, cur_submitted
            secondary_new, secondary_old = completed, cur_completed
            other_label = "完成时间"
        else:
            primary_new, primary_old = completed, cur_completed
            secondary_new, secondary_old = submitted, cur_submitted
            other_label = "提交时间"

        if primary_new < primary_old:
            # 旧回执：断线恢复重复提交旧结果的核心场景。
            if secondary_new > secondary_old:
                return "ignored", f"{other_label}较新但水位口径时间较旧，保留较新执行记录", {}
            return "ignored", "旧回执早于当前调度水位，已忽略，上次执行时间保持不变", {}
        if primary_new == primary_old and secondary_new <= secondary_old:
            return "ignored", "回执时间与当前水位一致，按重复提交忽略", {}

        # 时间前进：水位只进不退；提交/完成两个口径的较新值都记录下来。
        anchor = submitted if basis == "submitted_at" else completed
        last_execution = max(anchor, cur_last, floor)
        return "applied", "回执时间新于当前水位，已更新调度执行记录", {
            "last_execution": last_execution,
            "submitted_at": max(submitted, cur_submitted),
            "completed_at": max(completed, cur_completed),
        }

    def _apply_watermark(
        self,
        state: dict[str, Any],
        receipt: dict[str, Any],
        candidate: dict[str, Any],
        basis: str,
    ) -> dict[str, Any]:
        current = state.get("watermark")
        new_watermark = self._make_watermark(
            task_id=_int_id(receipt.get("task_id")),
            receipt_id=receipt.get("receipt_id"),
            last_execution=candidate["last_execution"],
            submitted_at=candidate["submitted_at"],
            completed_at=candidate["completed_at"],
            basis=basis,
            source="receipt",
        )
        state["watermarks"] = [new_watermark]
        return new_watermark

    def _persist_task_last_execution(self, entry: dict[str, Any], state: dict[str, Any]) -> None:
        current = state.get("watermark")
        floor = state.get("last_execution_floor") or datetime.min
        last_execution = None
        if current and current.get("last_execution"):
            last_execution = max(current["last_execution"], floor)
        elif floor > datetime.min:
            last_execution = floor
        if last_execution is not None:
            existing = parse_time(entry.get("上次执行"))
            if existing is None or last_execution >= existing:
                entry["上次执行"] = iso_time(last_execution)
        # 旧版本残留在任务行上的水位/回执字段一律清理，避免详情继续显示旧值。
        for key in LEGACY_WATERMARK_KEYS + LEGACY_RESIDUE_KEYS:
            entry.pop(key, None)

    # ------------------------------------------------------------------ 视图
    def _result(
        self,
        entry: dict[str, Any],
        state: dict[str, Any],
        receipt: dict[str, Any],
        *,
        applied: bool,
        duplicate: bool,
        recovery: bool,
        message: str,
    ) -> dict[str, Any]:
        return {
            "ok": True,
            "applied": applied,
            "ignored": not applied,
            "duplicate": duplicate,
            "recovery": recovery,
            "message": message,
            "entry": self.get_entry_detail(_int_id(entry.get("id"))),
            "receipt": self._receipt_view(receipt),
            "watermark": self._watermark_view(state.get("watermark")),
            "watermarkBasis": state["watermark"].get("basis") if state.get("watermark") else None,
            "水位依据": (
                BASIS_LABELS.get(state["watermark"].get("basis"), state["watermark"].get("basis"))
                if state.get("watermark") else None
            ),
            "lastExecution": entry.get("上次执行"),
            "上次执行": entry.get("上次执行"),
            "ignoredReceipts": [self._receipt_view(r) for r in state.get("ignored_receipts", [])],
            "executions": [self._receipt_view(r) for r in state.get("receipts", [])],
        }

    def _receipt_view(self, receipt: dict[str, Any]) -> dict[str, Any]:
        return {
            "id": receipt.get("id"),
            "taskId": receipt.get("task_id"),
            "receiptId": receipt.get("receipt_id"),
            "回执编号": receipt.get("receipt_id"),
            "submittedAt": receipt.get("submitted_at"),
            "提交时间": receipt.get("submitted_at"),
            "completedAt": receipt.get("completed_at"),
            "完成时间": receipt.get("completed_at"),
            "status": receipt.get("status"),
            "执行状态": receipt.get("status"),
            "result": receipt.get("result"),
            "执行结果": receipt.get("result"),
            "outcome": receipt.get("outcome"),
            "处理结果": receipt.get("outcome"),
            "reason": receipt.get("reason"),
            "忽略原因": receipt.get("reason") if receipt.get("outcome") == "ignored" else None,
            "recovery": bool(receipt.get("recovery")),
            "duplicateSubmissions": int(receipt.get("duplicate_submissions", 0)),
        }

    def _watermark_view(self, watermark: dict[str, Any] | None) -> dict[str, Any] | None:
        if watermark is None:
            return None
        basis = watermark.get("basis") or DEFAULT_WATERMARK_BASIS
        return {
            "id": watermark.get("id"),
            "taskId": watermark.get("task_id"),
            "receiptId": watermark.get("receipt_id"),
            "当前水位回执": watermark.get("receipt_id"),
            "lastExecution": iso_time(watermark.get("last_execution")),
            "上次执行": iso_time(watermark.get("last_execution")),
            "submittedAt": iso_time(watermark.get("submitted_at")),
            "最新提交时间": iso_time(watermark.get("submitted_at")),
            "completedAt": iso_time(watermark.get("completed_at")),
            "最新完成时间": iso_time(watermark.get("completed_at")),
            "basis": basis,
            "水位依据": BASIS_LABELS.get(basis, basis),
            "superseded": bool(watermark.get("superseded")),
            "source": watermark.get("source"),
        }

    # ------------------------------------------------------------------ 导出
    def export_rows(self) -> list[dict[str, Any]]:
        """导出：任务行带当前水位依据，被忽略的旧回执单独成行保留。"""
        rows: list[dict[str, Any]] = []
        for item in self.get_timeline():
            entry = store.find(MODULE, _int_id(item.get("id"))) or {}
            state = self._load_state(entry, persist=True)
            ignored = state["ignored_receipts"]
            rows.append({
                "记录类型": "调度任务",
                "任务ID": item.get("id"),
                "任务名称": item.get("name", ""),
                "状态": item.get("status", ""),
                "开始日期": item.get("start", ""),
                "结束日期": item.get("end", ""),
                "上次执行": item.get("lastExecution") or "",
                "水位依据": item.get("水位依据") or "",
                "当前水位回执": item.get("watermarkReceipt") or "",
                "最新提交时间": item.get("latestSubmittedAt") or "",
                "最新完成时间": item.get("latestCompletedAt") or "",
                "忽略回执数": len(ignored),
                "忽略回执编号": "、".join(str(r.get("receipt_id")) for r in ignored),
                "忽略回执提交时间": "、".join(str(r.get("submitted_at")) for r in ignored),
                "忽略回执完成时间": "、".join(str(r.get("completed_at")) for r in ignored),
                "忽略原因": "；".join(str(r.get("reason")) for r in ignored if r.get("reason")),
            })
            for receipt in ignored:
                rows.append({
                    "记录类型": "已忽略旧回执",
                    "任务ID": item.get("id"),
                    "任务名称": item.get("name", ""),
                    "状态": receipt.get("status", ""),
                    "开始日期": receipt.get("submitted_at", ""),
                    "结束日期": receipt.get("completed_at", ""),
                    "上次执行": item.get("lastExecution") or "",
                    "水位依据": item.get("水位依据") or "",
                    "当前水位回执": item.get("watermarkReceipt") or "",
                    "最新提交时间": receipt.get("submitted_at", ""),
                    "最新完成时间": receipt.get("completed_at", ""),
                    "忽略回执数": 1,
                    "忽略回执编号": receipt.get("receipt_id", ""),
                    "忽略回执提交时间": receipt.get("submitted_at", ""),
                    "忽略回执完成时间": receipt.get("completed_at", ""),
                    "忽略原因": receipt.get("reason", ""),
                })
        return rows
