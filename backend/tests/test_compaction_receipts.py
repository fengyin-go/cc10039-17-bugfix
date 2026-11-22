"""分表合并回执与保留清理联动测试：两张湖表复用同一任务编号的场景。

复现并验证修复：
- 完成范围按「任务编号 + 目标湖表」界定，同编号另一张表不再卡在待调度；
- 同表完成回执重复投递只记一次，归属对照区分两张同编号任务，
  已登记回执再次送达也不改变湖表来源；
- 保留策略读取同一份分表合并回执后才把关联旧分区放进可清理范围，
  清理记录逐项可查替代文件由哪张湖表写出。

注意：store 是全局单例，本文件使用专属编号与湖表，避免与种子数据互相影响。
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.store import store
from app.services.file_compaction import FileCompactionService
from app.services.lake_table import LakeTableService
from app.services.retention_mgr import RetentionMgrService

TASK_NO = "daily-compact"
INVENTORY_TABLE = "库存表"
SALES_TABLE = "销量表"
INVENTORY_PARTITION = "2026-11-04"  # 库存表四日分区
SALES_PARTITION = "2026-11-05"      # 销量表五日分区
INVENTORY_ADDR = ["s3://lake/inventory/dt=2026-11-04/compact-daily-compact-0001.parquet"]
SALES_ADDR = ["s3://lake/sales/dt=2026-11-05/compact-daily-compact-0002.parquet"]


def _ensure_lake_table(lake_svc: LakeTableService, name: str) -> dict:
    existing = store.find_by_field("lake_table", "表名", name)
    if existing:
        return existing[0]
    entry, missing = lake_svc.create_entry({
        "表名": name,
        "所属业务域": "交易域",
        "存储格式": "parquet",
        "分区键": "dt",
        "当前分片数": "4",
    })
    assert entry is not None and not missing
    lake_svc.register_table(entry["id"])
    return entry


def _create_compaction(comp_svc: FileCompactionService, table: str,
                       partition: str, addresses: list[str]) -> dict:
    entry, missing = comp_svc.create_entry({
        "任务编号": TASK_NO,
        "目标湖表": table,
        "文件阈值": "128",
        "合并后文件数": "",
        "执行状态": "",
        "目标分区": partition,
        "替代地址": addresses,
    })
    assert entry is not None and not missing
    return entry


def _create_retention(ret_svc: RetentionMgrService, name: str, table: str) -> dict:
    entry, missing = ret_svc.create_entry({
        "策略名称": name,
        "适用湖表": table,
        "保留天数": "30",
        "清理动作": "归档",
        "下次执行时间": "2026-11-23",
    })
    assert entry is not None and not missing
    enabled, msg = ret_svc.enable_retention(entry["id"])
    assert enabled is not None, f"启用保留策略失败: {msg}"
    return enabled


class TestDailyCompactScenario:
    """按验收顺序执行：库存四日完成 → 销量五日合并并回放库存回执
    → 保留策略对照两天替代地址 → 回执逆序再投递。"""

    def setup_method(self):
        self.comp = FileCompactionService()
        self.ret = RetentionMgrService()
        self.lake = LakeTableService()

    def test_01_inventory_day4_completes_and_shows_in_list(self):
        """沿用日结编号 daily-compact：库存表四日分区完成并在列表显示已完成。"""
        _ensure_lake_table(self.lake, INVENTORY_TABLE)
        entry = _create_compaction(self.comp, INVENTORY_TABLE, INVENTORY_PARTITION, INVENTORY_ADDR)

        triggered, msg = self.comp.trigger_compaction(entry["id"])
        assert triggered is not None, f"库存表合并触发失败: {msg}"

        completed, msg = self.comp.complete_compaction(entry["id"], "3")
        assert completed is not None, f"库存表合并完成失败: {msg}"
        assert completed["status"] == "已完成"

        # 合并任务列表里，库存表的日结任务显示已完成
        rows, total = self.comp.list_entries(filters={"任务编号": TASK_NO})
        inventory_rows = [r for r in rows if r.get("目标湖表") == INVENTORY_TABLE]
        assert len(inventory_rows) == 1
        assert inventory_rows[0]["status"] == "已完成"

        # 完成范围按表界定：库存表已完成，销量表尚未完成
        assert self.comp.is_task_completed(TASK_NO, INVENTORY_TABLE) is True
        assert self.comp.is_task_completed(TASK_NO, SALES_TABLE) is False

        # 分表合并回执已登记，替代地址为四日分区地址
        receipt = FileCompactionService.receipt_for(TASK_NO, INVENTORY_TABLE)
        assert receipt is not None
        assert receipt["partition"] == INVENTORY_PARTITION
        assert receipt["replacement_addresses"] == INVENTORY_ADDR

    def test_02_sales_day5_starts_and_inventory_receipt_replayed(self):
        """同一窗口：销量表五日分区启动合并，库存回执重复发送只记一次。"""
        _ensure_lake_table(self.lake, SALES_TABLE)
        entry = _create_compaction(self.comp, SALES_TABLE, SALES_PARTITION, SALES_ADDR)

        # 同编号不同湖表：第一张完成后，另一张这一轮仍可启动合并
        triggered, msg = self.comp.trigger_compaction(entry["id"])
        assert triggered is not None, f"销量表合并应可触发（同编号不应互相阻塞）: {msg}"

        # 第一张刚完成便遇到回执重复发送：同表回执只记一次
        before = len(FileCompactionService.receipts_for_task(TASK_NO))
        receipt, created, msg = FileCompactionService.deliver_completion_receipt(
            TASK_NO, INVENTORY_TABLE,
            partition=INVENTORY_PARTITION, replacement_addresses=INVENTORY_ADDR)
        assert created is False
        assert len(FileCompactionService.receipts_for_task(TASK_NO)) == before == 1
        assert receipt["table_name"] == INVENTORY_TABLE

        # 销量表五日分区完成合并，登记自己的回执
        completed, msg = self.comp.complete_compaction(entry["id"], "2")
        assert completed is not None, f"销量表合并完成失败: {msg}"
        assert completed["status"] == "已完成"

        # 归属对照：同一编号下两张表各自可查
        receipts = FileCompactionService.receipts_for_task(TASK_NO)
        assert len(receipts) == 2
        by_table = {r["table_name"]: r for r in receipts}
        assert by_table[INVENTORY_TABLE]["partition"] == INVENTORY_PARTITION
        assert by_table[SALES_TABLE]["partition"] == SALES_PARTITION
        assert by_table[SALES_TABLE]["replacement_addresses"] == SALES_ADDR

    def test_03_retention_cleanup_matches_receipts_per_table(self):
        """保留策略执行入口：两天的替代地址与清理结果分别对照。"""
        inv_retention = _create_retention(self.ret, "库存保留策略", INVENTORY_TABLE)
        sales_retention = _create_retention(self.ret, "销量保留策略", SALES_TABLE)

        # 库存表清理：读取库存回执，四日旧分区进入可清理范围
        _, msg_inv = self.ret.run_action(inv_retention["id"], "执行清理")
        inv_records = RetentionMgrService.cleanup_records(table_name=INVENTORY_TABLE)
        assert len(inv_records) == 1
        inv_items = inv_records[0]["items"]
        assert len(inv_items) == 1
        assert inv_items[0]["source_table"] == INVENTORY_TABLE
        assert inv_items[0]["partition"] == INVENTORY_PARTITION
        assert inv_items[0]["replacement_addresses"] == INVENTORY_ADDR
        assert inv_items[0]["cleaned_old_partitions"] == [INVENTORY_PARTITION]
        assert INVENTORY_PARTITION in msg_inv

        # 销量表清理：读取销量回执，五日旧分区进入可清理范围
        _, msg_sales = self.ret.run_action(sales_retention["id"], "执行清理")
        sales_records = RetentionMgrService.cleanup_records(table_name=SALES_TABLE)
        assert len(sales_records) == 1
        sales_items = sales_records[0]["items"]
        assert len(sales_items) == 1
        assert sales_items[0]["source_table"] == SALES_TABLE
        assert sales_items[0]["partition"] == SALES_PARTITION
        assert sales_items[0]["replacement_addresses"] == SALES_ADDR
        assert sales_items[0]["cleaned_old_partitions"] == [SALES_PARTITION]
        assert SALES_PARTITION in msg_sales

        # 对照：两天的替代地址不同，且各自指向本表回执登记的地址
        assert inv_items[0]["replacement_addresses"] != sales_items[0]["replacement_addresses"]
        inv_receipt = FileCompactionService.receipt_for(TASK_NO, INVENTORY_TABLE)
        sales_receipt = FileCompactionService.receipt_for(TASK_NO, SALES_TABLE)
        assert inv_items[0]["receipt_id"] == inv_receipt["receipt_id"]
        assert sales_items[0]["receipt_id"] == sales_receipt["receipt_id"]
        assert inv_items[0]["replacement_addresses"] == inv_receipt["replacement_addresses"]
        assert sales_items[0]["replacement_addresses"] == sales_receipt["replacement_addresses"]

    def test_04_reverse_order_redelivery_keeps_both_sources(self):
        """回执按相反次序再投递一次：两个来源仍各自可查，归属不变。"""
        sales_receipt = FileCompactionService.receipt_for(TASK_NO, SALES_TABLE)
        inv_receipt = FileCompactionService.receipt_for(TASK_NO, INVENTORY_TABLE)
        assert sales_receipt and inv_receipt

        # 逆序再投递：先销量、后库存
        again_sales, created_sales, _ = FileCompactionService.deliver_completion_receipt(
            TASK_NO, SALES_TABLE,
            partition=SALES_PARTITION, replacement_addresses=SALES_ADDR)
        assert created_sales is False
        # 库存回执按回执编号再次送达，即使声称来自销量表也不改变湖表来源
        again_inv, created_inv, _ = FileCompactionService.deliver_completion_receipt(
            TASK_NO, SALES_TABLE,
            partition=SALES_PARTITION, replacement_addresses=SALES_ADDR,
            receipt_id=inv_receipt["receipt_id"])
        assert created_inv is False
        assert again_inv["table_name"] == INVENTORY_TABLE

        # 归属对照仍是两条，两个来源各自可查
        receipts = FileCompactionService.receipts_for_task(TASK_NO)
        assert len(receipts) == 2
        inv_now = FileCompactionService.receipt_for(TASK_NO, INVENTORY_TABLE)
        sales_now = FileCompactionService.receipt_for(TASK_NO, SALES_TABLE)
        assert inv_now["table_name"] == INVENTORY_TABLE
        assert inv_now["replacement_addresses"] == INVENTORY_ADDR
        assert sales_now["table_name"] == SALES_TABLE
        assert sales_now["replacement_addresses"] == SALES_ADDR
        # 重复投递只记一次：回执编号不变
        assert inv_now["receipt_id"] == inv_receipt["receipt_id"]
        assert sales_now["receipt_id"] == sales_receipt["receipt_id"]


class TestCompletionScopeEdgeCases:
    """完成范围与回执归属的边界条件（使用独立编号，互不影响）。"""

    def setup_method(self):
        self.comp = FileCompactionService()
        self.ret = RetentionMgrService()
        self.lake = LakeTableService()

    def test_same_table_duplicate_trigger_blocked_but_other_table_allowed(self):
        """同表同编号重复触发仍被幂等拦截；同编号另一张表不受影响。"""
        task_no = "daily-compact-dup"
        table_a, table_b = "复用表A", "复用表B"
        entry_a, _ = self.comp.create_entry({
            "任务编号": task_no, "目标湖表": table_a, "文件阈值": "64",
            "合并后文件数": "", "执行状态": "",
        })
        self.comp.trigger_compaction(entry_a["id"])
        completed, _ = self.comp.complete_compaction(entry_a["id"], "1")
        assert completed is not None

        # 同表同编号：新任务不可再触发
        dup, _ = self.comp.create_entry({
            "任务编号": task_no, "目标湖表": table_a, "文件阈值": "64",
            "合并后文件数": "", "执行状态": "",
        })
        blocked, msg = self.comp.trigger_compaction(dup["id"])
        assert blocked is None
        assert "已经完成" in msg
        assert dup["status"] == "待调度"

        # 同编号另一张表：可以触发并完成（修复前会卡在待调度）
        entry_b, _ = self.comp.create_entry({
            "任务编号": task_no, "目标湖表": table_b, "文件阈值": "64",
            "合并后文件数": "", "执行状态": "",
        })
        triggered, msg = self.comp.trigger_compaction(entry_b["id"])
        assert triggered is not None, f"同编号另一张表应可触发: {msg}"
        completed_b, _ = self.comp.complete_compaction(entry_b["id"], "1")
        assert completed_b is not None
        assert self.comp.is_task_completed(task_no, table_a) is True
        assert self.comp.is_task_completed(task_no, table_b) is True

    def test_cleanup_without_receipt_has_empty_scope(self):
        """未读到分表合并回执时，旧分区不进入可清理范围。"""
        table = "无回执表"
        _ensure_lake_table(self.lake, table)
        retention = _create_retention(self.ret, "无回执表保留策略", table)

        entry, msg = self.ret.execute_cleanup(retention["id"])
        assert entry is not None
        assert "可清理范围为空" in msg
        records = RetentionMgrService.cleanup_records(table_name=table)
        assert len(records) == 1
        assert records[0]["items"] == []
        assert records[0]["cleaned_partition_count"] == 0

    def test_receipt_replay_cannot_change_table_attribution(self):
        """已登记回执再次送达（按回执编号）不改变湖表来源。"""
        task_no = "daily-compact-attr"
        receipt, created, _ = FileCompactionService.deliver_completion_receipt(
            task_no, "来源表甲", partition="2026-11-06",
            replacement_addresses=["s3://lake/a/dt=2026-11-06/c.parquet"])
        assert created is True

        replayed, created, msg = FileCompactionService.deliver_completion_receipt(
            task_no, "冒名表乙", partition="2026-11-07",
            replacement_addresses=["s3://lake/b/dt=2026-11-07/c.parquet"],
            receipt_id=receipt["receipt_id"])
        assert created is False
        assert replayed["table_name"] == "来源表甲"
        assert replayed["partition"] == "2026-11-06"
        # 归属对照里不出现冒名表
        assert FileCompactionService.receipt_for(task_no, "冒名表乙") is None
        assert len(FileCompactionService.receipts_for_task(task_no)) == 1
