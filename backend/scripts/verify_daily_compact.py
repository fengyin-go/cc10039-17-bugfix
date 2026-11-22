"""验收脚本：两张湖表复用日结编号 daily-compact 的合并与保留清理全流程。

步骤对应验收口径：
  1) 沿用日结编号 daily-compact，让库存表四日分区在合并任务列表里显示完成；
  2) 发起销量表五日分区合并，并在同一窗口回放库存回执；
  3) 从保留策略执行入口分别对照两天的替代地址和清理结果；
  4) 把回执按相反次序再投递一次，确认两个来源仍各自可查。

运行：cd backend && .venv/bin/python scripts/verify_daily_compact.py
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
INVENTORY = "库存表"
SALES = "销量表"
DAY4 = "2026-11-04"   # 库存表四日分区
DAY5 = "2026-11-05"   # 销量表五日分区
DAY4_ADDR = ["s3://lake/inventory/dt=2026-11-04/compact-daily-compact-0001.parquet"]
DAY5_ADDR = ["s3://lake/sales/dt=2026-11-05/compact-daily-compact-0002.parquet"]

comp = FileCompactionService()
ret = RetentionMgrService()
lake = LakeTableService()

failures: list[str] = []


def check(label: str, ok: bool, detail: str = "") -> None:
    mark = "✅" if ok else "❌"
    print(f"  {mark} {label}" + (f" —— {detail}" if detail else ""))
    if not ok:
        failures.append(label)


def ensure_table(name: str) -> None:
    if store.find_by_field("lake_table", "表名", name):
        return
    entry, _ = lake.create_entry({
        "表名": name, "所属业务域": "交易域", "存储格式": "parquet",
        "分区键": "dt", "当前分片数": "4",
    })
    lake.register_table(entry["id"])


def add_compaction(table: str, partition: str, addresses: list[str]) -> dict:
    entry, missing = comp.create_entry({
        "任务编号": TASK_NO, "目标湖表": table, "文件阈值": "128",
        "合并后文件数": "", "执行状态": "",
        "目标分区": partition, "替代地址": addresses,
    })
    assert entry is not None and not missing
    return entry


def add_retention(name: str, table: str) -> dict:
    entry, _ = ret.create_entry({
        "策略名称": name, "适用湖表": table, "保留天数": "30",
        "清理动作": "归档", "下次执行时间": "2026-11-23",
    })
    enabled, msg = ret.enable_retention(entry["id"])
    assert enabled is not None, msg
    return enabled


print("=" * 72)
print("步骤 1：沿用日结编号 daily-compact，库存表四日分区完成合并")
print("=" * 72)
ensure_table(INVENTORY)
inv_task = add_compaction(INVENTORY, DAY4, DAY4_ADDR)
_, msg = comp.trigger_compaction(inv_task["id"])
print(f"  触发：{msg}")
_, msg = comp.complete_compaction(inv_task["id"], "3")
print(f"  完成：{msg}")

rows, _ = comp.list_entries(filters={"任务编号": TASK_NO})
inv_row = [r for r in rows if r.get("目标湖表") == INVENTORY][0]
check("合并任务列表：库存表四日分区显示已完成", inv_row["status"] == "已完成",
      f"状态={inv_row['status']}，合并后文件数={inv_row.get('合并后文件数')}")
check("完成范围按表界定：库存表已完成 / 销量表未完成",
      comp.is_task_completed(TASK_NO, INVENTORY) and not comp.is_task_completed(TASK_NO, SALES))

print()
print("=" * 72)
print("步骤 2：发起销量表五日分区合并，同一窗口回放库存回执")
print("=" * 72)
ensure_table(SALES)
sales_task = add_compaction(SALES, DAY5, DAY5_ADDR)
triggered, msg = comp.trigger_compaction(sales_task["id"])
check("同编号第二张表不再卡在待调度", triggered is not None, msg)

receipt, created, msg = FileCompactionService.deliver_completion_receipt(
    TASK_NO, INVENTORY, partition=DAY4, replacement_addresses=DAY4_ADDR)
check("库存回执重复发送只记一次", not created, msg)
check("归属对照仍只有库存一条", len(FileCompactionService.receipts_for_task(TASK_NO)) == 1)

_, msg = comp.complete_compaction(sales_task["id"], "2")
print(f"  完成：{msg}")
receipts = FileCompactionService.receipts_for_task(TASK_NO)
check("归属对照区分两张同编号任务", len(receipts) == 2,
      "；".join(f"{r['table_name']}→{r['receipt_id']}({r['partition']})" for r in receipts))

print()
print("=" * 72)
print("步骤 3：从保留策略执行入口分别对照两天的替代地址和清理结果")
print("=" * 72)
inv_ret = add_retention("库存保留策略", INVENTORY)
sales_ret = add_retention("销量保留策略", SALES)

_, msg_inv = ret.run_action(inv_ret["id"], "执行清理")
_, msg_sales = ret.run_action(sales_ret["id"], "执行清理")
print(f"  库存清理：{msg_inv}")
print(f"  销量清理：{msg_sales}")

inv_rec = RetentionMgrService.cleanup_records(table_name=INVENTORY)[0]
sales_rec = RetentionMgrService.cleanup_records(table_name=SALES)[0]
inv_item, sales_item = inv_rec["items"][0], sales_rec["items"][0]
check("库存清理记录逐项可查：四日替代地址来自库存表",
      inv_item["source_table"] == INVENTORY and inv_item["replacement_addresses"] == DAY4_ADDR,
      f"{inv_item['partition']} → {inv_item['replacement_addresses'][0]}")
check("销量清理记录逐项可查：五日替代地址来自销量表",
      sales_item["source_table"] == SALES and sales_item["replacement_addresses"] == DAY5_ADDR,
      f"{sales_item['partition']} → {sales_item['replacement_addresses'][0]}")
check("两天替代地址不互相覆盖",
      inv_item["replacement_addresses"] != sales_item["replacement_addresses"])
check("清理记录与合并回执对得上",
      inv_item["receipt_id"] == FileCompactionService.receipt_for(TASK_NO, INVENTORY)["receipt_id"]
      and sales_item["receipt_id"] == FileCompactionService.receipt_for(TASK_NO, SALES)["receipt_id"])

print()
print("=" * 72)
print("步骤 4：回执按相反次序再投递一次（先销量、后库存）")
print("=" * 72)
inv_rid = FileCompactionService.receipt_for(TASK_NO, INVENTORY)["receipt_id"]
sales_rid = FileCompactionService.receipt_for(TASK_NO, SALES)["receipt_id"]

_, created_sales, msg = FileCompactionService.deliver_completion_receipt(
    TASK_NO, SALES, partition=DAY5, replacement_addresses=DAY5_ADDR)
check("销量回执再投递被去重", not created_sales, msg)

# 库存回执按回执编号再次送达，即使声称来自销量表也不改变湖表来源
replayed, created_inv, msg = FileCompactionService.deliver_completion_receipt(
    TASK_NO, SALES, partition=DAY5, replacement_addresses=DAY5_ADDR, receipt_id=inv_rid)
check("库存回执再次送达不改变湖表来源",
      not created_inv and replayed["table_name"] == INVENTORY, msg)

receipts = FileCompactionService.receipts_for_task(TASK_NO)
inv_now = FileCompactionService.receipt_for(TASK_NO, INVENTORY)
sales_now = FileCompactionService.receipt_for(TASK_NO, SALES)
check("两个来源仍各自可查", len(receipts) == 2
      and inv_now["receipt_id"] == inv_rid and inv_now["replacement_addresses"] == DAY4_ADDR
      and sales_now["receipt_id"] == sales_rid and sales_now["replacement_addresses"] == DAY5_ADDR,
      "；".join(f"{r['table_name']}={r['receipt_id']}(投递{r['delivery_count']}次)" for r in receipts))

print()
if failures:
    print(f"验收未通过：{len(failures)} 项失败 —— {'、'.join(failures)}")
    sys.exit(1)
print("验收通过：同编号两张湖表的合并完成范围、分表回执与保留清理记录全部对得上。")
