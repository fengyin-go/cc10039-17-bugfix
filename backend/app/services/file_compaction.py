"""小文件合并领域服务：调度和执行小文件合并任务以减少元数据开销。

小文件合并具备幂等追踪能力：同一湖表的同一批次合并任务不会重复执行。
完成范围按「任务编号 + 目标湖表」界定：两张湖表复用同一任务编号（如日结编号）
时各自完成、互不影响，不会把另一张表的任务卡在待调度。

合并完成后登记「分表合并回执」：同一 (任务编号, 湖表) 的回执重复投递只记一次，
归属对照按任务编号 + 湖表区分，已登记的回执再次送达也不会改变湖表来源。
保留策略读取同一份回执后，才把关联旧分区放进可清理范围。

上游接收冷热分层迁移事件，下游联动保留策略的清理动作。
"""
from __future__ import annotations

import datetime
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class FileCompactionService(BaseModuleService):
    MODULE = "file_compaction"
    ENTITY = "合并任务"
    OPERATION_TERM = "调度"
    LIST_FIELDS = ["任务编号", "目标湖表", "文件阈值", "合并后文件数", "执行状态"]
    REQUIRED_FIELDS = ["任务编号", "目标湖表", "文件阈值"]
    STATUS_ORDER = ["待调度", "执行中", "已完成", "执行失败"]
    ACTION_RULES = {"触发合并": "执行中", "取消任务": "执行失败"}
    NEGATIVE_ACTIONS = []

    # 列表字段之外的扩展信息：登记任务时可一并携带，完成时写入分表合并回执
    EXTRA_FIELDS = ["目标分区", "替代地址"]

    # 幂等追踪：已完成的 (任务编号, 目标湖表) 集合 —— 完成范围按表界定
    _completed_scopes: set[tuple[str, str]] = set()
    # 分表合并回执登记簿：(任务编号, 目标湖表) → 回执，同表重复投递只记一次
    _completion_receipts: dict[tuple[str, str], dict[str, Any]] = {}
    # 回执编号索引：回执再次送达时归属（湖表来源）以首次登记为准
    _receipts_by_id: dict[str, dict[str, Any]] = {}
    _receipt_seq: int = 0

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        """登记合并任务，允许随任务携带目标分区、替代地址等扩展信息。"""
        entry, missing = super().create_entry(values)
        if entry is not None:
            for field in self.EXTRA_FIELDS:
                if values.get(field) not in (None, ""):
                    entry[field] = values.get(field)
        return entry, missing

    def trigger_compaction(self, entry_id: int) -> tuple[dict[str, Any] | None, str]:
        """触发小文件合并任务，支持幂等追踪。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, "合并任务不存在"
        if entry.get("status") != "待调度":
            return None, f"合并任务状态为{entry.get('status')}，不可触发"

        task_no = entry.get("任务编号", "")
        table_name = entry.get("目标湖表", "")

        # 幂等检查：同一任务编号在同一湖表上已完成的不再触发；
        # 不同湖表复用同一编号互不影响（完成范围按表界定）
        if (task_no, table_name) in self._completed_scopes:
            return None, f"任务 {task_no} 在湖表 {table_name} 上已经完成，不可重复触发"

        # 检查同湖表是否有其他执行中的合并
        active_compactions = [
            c for c in store.rows(self.MODULE)
            if c.get("目标湖表") == table_name
            and c.get("status") == "执行中"
            and c.get("id") != entry_id
        ]
        if active_compactions:
            return None, f"湖表 {table_name} 已有合并任务在执行中（任务编号：{active_compactions[0].get('任务编号')}），不可并发触发"

        # 自身状态流转
        entry["status"] = "执行中"
        entry["pending"] = True
        entry["abnormal"] = False

        store.log_event("compaction_triggered", "file_compaction", "file_compaction", {
            "task_no": task_no,
            "table_name": table_name,
            "file_threshold": entry.get("文件阈值", ""),
        })

        return entry, f"合并任务 {task_no} 已触发"

    def complete_compaction(
        self,
        entry_id: int,
        result_file_count: str = "",
        partition: str | None = None,
        replacement_addresses: list[str] | None = None,
    ) -> tuple[dict[str, Any] | None, str]:
        """完成合并任务：按表记录完成范围，登记分表合并回执，通知保留策略清理旧文件。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, "合并任务不存在"
        if entry.get("status") != "执行中":
            return None, f"合并任务状态为{entry.get('status')}，不可完成"

        task_no = entry.get("任务编号", "")
        table_name = entry.get("目标湖表", "")
        if partition is None:
            partition = entry.get("目标分区", "")
        addresses = self._normalize_addresses(
            replacement_addresses, entry, task_no, table_name, partition)

        entry["status"] = "已完成"
        entry["pending"] = False
        entry["abnormal"] = False
        if result_file_count:
            entry["合并后文件数"] = result_file_count

        # 幂等追踪：完成范围按 (任务编号, 湖表) 界定
        self._completed_scopes.add((task_no, table_name))

        # 登记分表合并回执（同表重复完成只记一次，归属以首次登记为准）
        receipt, _, _ = self.deliver_completion_receipt(
            task_no,
            table_name,
            partition=partition,
            replacement_addresses=addresses,
            result_file_count=result_file_count or str(entry.get("合并后文件数", "")),
            source_entry_id=entry_id,
        )

        store.log_event("compaction_completed", "file_compaction", "retention_mgr", {
            "task_no": task_no,
            "table_name": table_name,
            "partition": partition,
            "receipt_id": receipt["receipt_id"],
            "result_file_count": result_file_count,
        })

        # 通知保留策略：合并完成后的旧文件可清理
        retentions = store.find_by_field("retention_mgr", "适用湖表", table_name)
        for r in retentions:
            if r.get("status") == "已生效":
                r["pending"] = True

        return entry, (
            f"合并任务 {task_no}（湖表 {table_name}）已完成，结果文件数={result_file_count}，"
            f"回执={receipt['receipt_id']}，已通知保留策略清理旧文件"
        )

    @staticmethod
    def _normalize_addresses(
        replacement_addresses: list[str] | None,
        entry: dict[str, Any],
        task_no: str,
        table_name: str,
        partition: str,
    ) -> list[str]:
        """汇总替代地址：优先显式入参，其次任务登记信息，最后按约定路径生成。"""
        if replacement_addresses:
            return [str(a) for a in replacement_addresses]
        raw = entry.get("替代地址")
        if isinstance(raw, str) and raw.strip():
            return [a.strip() for a in raw.split(",") if a.strip()]
        if isinstance(raw, (list, tuple)) and raw:
            return [str(a) for a in raw]
        suffix = f"dt={partition}" if partition else "compact"
        return [f"s3://lake/{table_name}/{suffix}/compact-{task_no}.parquet"]

    @classmethod
    def deliver_completion_receipt(
        cls,
        task_no: str,
        table_name: str,
        *,
        partition: str = "",
        replacement_addresses: list[str] | None = None,
        result_file_count: str = "",
        receipt_id: str | None = None,
        source_entry_id: int | None = None,
    ) -> tuple[dict[str, Any], bool, str]:
        """投递分表合并完成回执。

        - 同一 (任务编号, 湖表) 的回执重复投递只记一次；
        - 归属对照按 (任务编号, 湖表) 区分，同编号不同湖表各自登记；
        - 已登记的回执再次送达（按回执编号）时不改变其湖表来源。

        返回 (回执, 是否新登记, 消息)。
        """
        # 按回执编号命中：再次送达不改变已登记的湖表来源
        if receipt_id:
            known = cls._receipts_by_id.get(receipt_id)
            if known is not None:
                known["delivery_count"] += 1
                store.log_event("compaction_receipt_replayed", "file_compaction", "file_compaction", {
                    "receipt_id": receipt_id,
                    "claimed_table_name": table_name,
                    "recorded_table_name": known["table_name"],
                    "deduped": True,
                })
                return known, False, (
                    f"回执 {receipt_id} 已登记过（湖表 {known['table_name']}），"
                    f"重复送达已忽略，湖表来源不变"
                )

        key = (task_no, table_name)
        existing = cls._completion_receipts.get(key)
        if existing is not None:
            existing["delivery_count"] += 1
            store.log_event("compaction_receipt_replayed", "file_compaction", "file_compaction", {
                "receipt_id": existing["receipt_id"],
                "task_no": task_no,
                "table_name": table_name,
                "deduped": True,
            })
            return existing, False, (
                f"任务 {task_no} 在湖表 {table_name} 的回执已登记过"
                f"（{existing['receipt_id']}），重复投递已忽略"
            )

        cls._receipt_seq += 1
        rid = receipt_id or f"RCP-{cls._receipt_seq:04d}"
        receipt = {
            "receipt_id": rid,
            "task_no": task_no,
            "table_name": table_name,
            "partition": partition,
            "replacement_addresses": [str(a) for a in (replacement_addresses or [])],
            "result_file_count": result_file_count,
            "source_entry_id": source_entry_id,
            "delivery_count": 1,
            "recorded_at": datetime.datetime.now().isoformat(),
        }
        cls._completion_receipts[key] = receipt
        cls._receipts_by_id[rid] = receipt
        store.log_event("compaction_receipt_recorded", "file_compaction", "retention_mgr", {
            "receipt_id": rid,
            "task_no": task_no,
            "table_name": table_name,
            "partition": partition,
            "replacement_addresses": receipt["replacement_addresses"],
        })
        return receipt, True, f"分表合并回执 {rid} 已登记（任务 {task_no}，湖表 {table_name}）"

    def is_task_completed(self, task_no: str, table_name: str | None = None) -> bool:
        """幂等检查：任务编号在指定湖表上是否已完成；不传湖表时查询该编号是否任一表已完成。"""
        if table_name is not None:
            return (task_no, table_name) in self._completed_scopes
        return any(no == task_no for no, _ in self._completed_scopes)

    @classmethod
    def all_receipts(cls) -> list[dict[str, Any]]:
        """全量分表合并回执（归属对照表）。"""
        return list(cls._completion_receipts.values())

    @classmethod
    def receipt_for(cls, task_no: str, table_name: str) -> dict[str, Any] | None:
        """按 (任务编号, 湖表) 查询分表合并回执。"""
        return cls._completion_receipts.get((task_no, table_name))

    @classmethod
    def receipt_by_id(cls, receipt_id: str) -> dict[str, Any] | None:
        """按回执编号查询回执。"""
        return cls._receipts_by_id.get(receipt_id)

    @classmethod
    def receipts_for_task(cls, task_no: str) -> list[dict[str, Any]]:
        """归属对照：查询同一任务编号下各湖表各自的回执。"""
        return [r for (no, _), r in cls._completion_receipts.items() if no == task_no]

    @classmethod
    def receipts_for_table(cls, table_name: str) -> list[dict[str, Any]]:
        """查询指定湖表的全部分表合并回执，供保留策略划定可清理范围。"""
        return [r for (_, t), r in cls._completion_receipts.items() if t == table_name]

    def compaction_status_for_table(self, table_name: str) -> dict[str, Any]:
        """查询指定湖表的合并任务状态摘要。"""
        compactions = store.find_by_field(self.MODULE, "目标湖表", table_name)
        return {
            "table_name": table_name,
            "total_tasks": len(compactions),
            "pending": sum(1 for c in compactions if c.get("status") == "待调度"),
            "running": sum(1 for c in compactions if c.get("status") == "执行中"),
            "completed": sum(1 for c in compactions if c.get("status") == "已完成"),
            "failed": sum(1 for c in compactions if c.get("status") == "执行失败"),
        }

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        if action == "触发合并":
            return self.trigger_compaction(entry_id)
        if action == "完成合并":
            # 完成回执所需的目标分区/替代地址取自任务登记信息
            return self.complete_compaction(entry_id)
        return super().run_action(entry_id, action)
