#!/usr/bin/env python3
"""初始化演示表：生成 2000 条运单，按大区分区写入真实列存。

用法: python3 scripts/init_table.py [数据目录] [行数]
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from colstore import Table
from colstore.demo_data import SCHEMA, generate_rows
from colstore.partitions import get_partitioner


def main() -> None:
    root = sys.argv[1] if len(sys.argv) > 1 else os.path.join(os.getcwd(), "data_store")
    count = int(sys.argv[2]) if len(sys.argv) > 2 else 2000
    if os.path.exists(os.path.join(root, "state.json")):
        print(f"表已存在: {root}，如需重建请先删除该目录")
        sys.exit(1)
    table = Table(root, schema=SCHEMA, table_name="物流运单", keep_generations=3)
    rows = generate_rows(count)
    gen = table.create(rows, get_partitioner("by_region"), actor="seed-script")
    stats = table.stats()
    print(f"建表完成: {root}")
    print(f"  当前代: {gen}，记录数: {stats['row_count']}，段数: {len(stats['segments'])}")
    print(f"  列存字节: {stats['data_bytes']}，索引字节: {stats['index_bytes']}")


if __name__ == "__main__":
    main()
