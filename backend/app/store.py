"""内存数据仓库：给每个业务模块准备一份可筛选、可流转的示例数据。

真实项目里这里会换成数据库访问层；当前实现只依赖标准库，保证克隆下来就能起。

调度回执、调度水位这类"配套表"放在 ``aux`` 里：它们属于业务模块的内部一致性数据，
不参与概览页的模块计数，但可以和主表放进同一个 :meth:`Store.transaction` 里一次性提交。
"""
from __future__ import annotations

import copy
import threading
from contextlib import contextmanager
from typing import Any, Iterator

from app.seed import SEED_ROWS


class Store:
    def __init__(self) -> None:
        self.lock = threading.RLock()
        self._tables: dict[str, list[dict[str, Any]]] = {
            name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()
        }
        self._aux: dict[str, list[dict[str, Any]]] = {}

    def module_names(self) -> list[str]:
        return sorted(self._tables)

    def rows(self, module: str) -> list[dict[str, Any]]:
        return self._tables.setdefault(module, [])

    def aux_rows(self, name: str) -> list[dict[str, Any]]:
        """配套表：调度回执、调度水位等，不计入业务模块统计。"""
        return self._aux.setdefault(name, [])

    def find(self, module: str, entry_id: int) -> dict[str, Any] | None:
        for row in self.rows(module):
            if int(row.get("id", 0)) == entry_id:
                return row
        return None

    def reset(self) -> None:
        """把内存数据恢复到启动时的示例数据，供测试或本地重置使用。"""
        with self.lock:
            self._tables = {
                name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()
            }
            self._aux = {}

    @contextmanager
    def transaction(self, *names: str) -> Iterator[None]:
        """一次提交多张表：任一步失败就整体回滚，避免"时间写了、结果没写"。

        当前仓库是内存实现，事务边界通过在进入时快照、异常时恢复来表达；换成数据库时
        这里对应一个真实事务。调用方需要把"执行结果"和"上次执行时间"放在同一个
        ``with store.transaction(...)`` 里写入。
        """
        with self.lock:
            snapshots: dict[str, tuple[str, list[dict[str, Any]]]] = {}
            for name in names:
                if name in self._tables:
                    snapshots[name] = ("main", copy.deepcopy(self._tables[name]))
                else:
                    snapshots[name] = ("aux", copy.deepcopy(self._aux.get(name, [])))
            try:
                yield
            except BaseException:
                for name, (kind, snapshot) in snapshots.items():
                    if kind == "main":
                        self._tables[name] = snapshot
                    else:
                        self._aux[name] = snapshot
                raise

    def overview(self) -> dict[str, object]:
        modules: list[dict[str, object]] = []
        for name in self.module_names():
            rows = self.rows(name)
            modules.append({
                "name": name,
                "created": len(rows),
                "pending": sum(1 for row in rows if row.get("pending")),
                "abnormal": sum(1 for row in rows if row.get("abnormal")),
            })
        cards = [
            {"label": "业务模块", "value": len(modules)},
            {"label": "今日新增", "value": sum(int(item["created"]) for item in modules)},
            {"label": "待处理", "value": sum(int(item["pending"]) for item in modules)},
            {"label": "异常量", "value": sum(int(item["abnormal"]) for item in modules)},
        ]
        return {"cards": cards, "modules": modules}


store = Store()
