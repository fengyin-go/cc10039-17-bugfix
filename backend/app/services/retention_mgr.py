"""保留策略领域服务：管理过期数据的自动清理、归档和保留策略。

上游接收小文件合并完成事件，下游联动存储成本分析和治理报告。
保留策略是数据生命周期管理的最后一道防线。

清理范围以分表合并回执为准：保留策略读到本湖表自己的回执后，
关联旧分区才进入可清理范围；清理记录逐项标注替代文件由哪张湖表写出、
替代地址是什么，保证清理和合并记录逐项对得上。
"""
from __future__ import annotations

import datetime
from typing import Any

from app.services.base import BaseModuleService
from app.services.file_compaction import FileCompactionService
from app.store import store


class RetentionMgrService(BaseModuleService):
    MODULE = "retention_mgr"
    ENTITY = "保留策略"
    OPERATION_TERM = "归档"
    LIST_FIELDS = ["策略名称", "适用湖表", "保留天数", "清理动作", "下次执行时间"]
    REQUIRED_FIELDS = ["策略名称", "适用湖表", "保留天数"]
    STATUS_ORDER = ["待生效", "已生效", "已暂停"]
    ACTION_RULES = {"启用保留": "已生效", "暂停保留": "已暂停"}
    NEGATIVE_ACTIONS = []

    # 清理执行记录：逐项可查（替代文件来源湖表、替代地址、被清理旧分区）
    _cleanup_records: list[dict[str, Any]] = []
    _cleanup_seq: int = 0

    def enable_retention(self, entry_id: int) -> tuple[dict[str, Any] | None, str]:
        """启用保留策略：验证湖表已注册并设置清理计划。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, "保留策略不存在"
        if entry.get("status") != "待生效":
            return None, f"保留策略状态为{entry.get('status')}，不可启用"

        table_name = entry.get("适用湖表", "")
        retention_days = entry.get("保留天数", "")

        # 验证湖表已注册
        lake_tables = store.find_by_field("lake_table", "表名", table_name)
        if not lake_tables:
            return None, f"湖表 {table_name} 不存在，不可启用保留策略"

        entry["status"] = "已生效"
        entry["pending"] = False
        entry["abnormal"] = False

        store.log_event("retention_enabled", "retention_mgr", "lake_table", {
            "retention_id": entry_id,
            "table_name": table_name,
            "retention_days": retention_days,
        })

        return entry, f"保留策略 {entry.get('策略名称')} 已生效，保留天数={retention_days}"

    def pause_retention(self, entry_id: int) -> tuple[dict[str, Any] | None, str]:
        """暂停保留策略：记录当前检查点后暂停。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, "保留策略不存在"
        if entry.get("status") != "已生效":
            return None, f"保留策略状态为{entry.get('status')}，不可暂停"

        # 创建检查点
        snapshot_key = store.create_snapshot(self.MODULE)

        entry["status"] = "已暂停"
        entry["pending"] = False
        entry["abnormal"] = False

        store.log_event("retention_paused", "retention_mgr", "retention_mgr", {
            "retention_id": entry_id,
            "snapshot_key": snapshot_key,
        })

        return entry, f"保留策略 {entry.get('策略名称')} 已暂停，检查点={snapshot_key}"

    def execute_cleanup(self, entry_id: int) -> tuple[dict[str, Any] | None, str]:
        """执行清理动作：按保留天数清理过期分区数据。

        先读取本湖表的分表合并回执（与小文件合并登记的是同一份），
        只有回执覆盖的关联旧分区才进入可清理范围；清理记录逐项标注
        替代文件来源湖表和替代地址，供事后对照合并记录。
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, "保留策略不存在"
        if entry.get("status") != "已生效":
            return None, f"保留策略状态为{entry.get('status')}，不可执行清理"

        table_name = entry.get("适用湖表", "")
        retention_days = entry.get("保留天数", "7")
        cleanup_action = entry.get("清理动作", "归档")

        # 读取本湖表的分表合并回执，据此划定可清理范围
        receipts = FileCompactionService.receipts_for_table(table_name)
        cleanable_items = []
        for r in receipts:
            partition = r.get("partition", "")
            cleanable_items.append({
                "receipt_id": r["receipt_id"],
                "task_no": r["task_no"],
                # 替代文件由哪张湖表写出：以回执首次登记的归属为准
                "source_table": r["table_name"],
                "partition": partition,
                "replacement_addresses": list(r.get("replacement_addresses", [])),
                "cleaned_old_partitions": [partition] if partition else [],
            })

        RetentionMgrService._cleanup_seq += 1
        record = {
            "cleanup_id": f"CLEAN-{RetentionMgrService._cleanup_seq:04d}",
            "retention_id": entry_id,
            "策略名称": entry.get("策略名称", ""),
            "table_name": table_name,
            "retention_days": retention_days,
            "action": cleanup_action,
            "items": cleanable_items,
            "cleaned_partition_count": sum(
                len(item["cleaned_old_partitions"]) for item in cleanable_items),
            "executed_at": datetime.datetime.now().isoformat(),
        }
        self._cleanup_records.append(record)

        # 记录清理操作
        store.log_event("retention_cleanup_executed", "retention_mgr", "govern_report", {
            "retention_id": entry_id,
            "table_name": table_name,
            "retention_days": retention_days,
            "action": cleanup_action,
            "cleanup_id": record["cleanup_id"],
            "receipt_ids": [item["receipt_id"] for item in cleanable_items],
            "cleaned_partitions": record["cleaned_partition_count"],
        })

        # 更新成本报告中的存储释放信息
        cost_reports = store.find_by_field("cost_report", "目标业务域", entry.get("适用湖表", ""))
        for cr in cost_reports:
            cr["pending"] = True
        if cost_reports:
            store.log_event("retention_to_cost", "retention_mgr", "cost_report", {
                "retention_id": entry_id,
                "affected_reports": len(cost_reports),
            })

        if cleanable_items:
            scope_desc = "、".join(
                f"{item['partition']}（回执 {item['receipt_id']}）" for item in cleanable_items)
            message = (
                f"保留策略 {entry.get('策略名称')} 清理完成，操作={cleanup_action}，"
                f"保留={retention_days}天，可清理旧分区：{scope_desc}"
            )
        else:
            message = (
                f"保留策略 {entry.get('策略名称')} 清理完成，操作={cleanup_action}，"
                f"保留={retention_days}天，未读到分表合并回执，可清理范围为空"
            )
        return entry, message

    @classmethod
    def cleanup_records(
        cls,
        entry_id: int | None = None,
        table_name: str | None = None,
    ) -> list[dict[str, Any]]:
        """查询清理执行记录，可按保留策略或湖表过滤，逐项对照合并回执。"""
        records = list(cls._cleanup_records)
        if entry_id is not None:
            records = [r for r in records if r.get("retention_id") == entry_id]
        if table_name:
            records = [r for r in records if r.get("table_name") == table_name]
        return records

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        if action == "启用保留":
            return self.enable_retention(entry_id)
        if action == "暂停保留":
            return self.pause_retention(entry_id)
        if action == "执行清理":
            return self.execute_cleanup(entry_id)
        return super().run_action(entry_id, action)
