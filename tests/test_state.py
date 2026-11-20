"""状态恢复、保留策略与索引成本测试。"""
import json
import os
import shutil
import sys
import tempfile
import time
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from colstore import Table
from colstore.demo_data import SCHEMA, generate_rows
from colstore.index import HEADER as IDX_HEADER
from colstore.index import RECORD_SIZE as IDX_RECORD_SIZE
from colstore.index import IndexReader
from colstore.partitions import get_partitioner, make_shuffle
from colstore.state import StateStore


class TestStateRecovery(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="colstore-state-")
        # keep=1：当前代 + 1 个历史代，窗口最小，GC 行为最容易断言
        self.table = Table(self.root, schema=SCHEMA, table_name="t", keep_generations=1)
        self.table.create(generate_rows(80, seed=3), get_partitioner("by_region"))
        self.table.compact(make_shuffle(1), actor="t")
        self.table.compact(make_shuffle(2), actor="t")

    def tearDown(self):
        self.table.close()
        shutil.rmtree(self.root, ignore_errors=True)

    def test_corrupt_state_json_rebuilds_from_manifest_chain(self):
        rid = self.table.scan(limit=1)[0].rid
        # 模拟 state.json 写入中途崩溃：内容截断
        with open(os.path.join(self.root, "state.json"), "w") as fh:
            fh.write('{"current_generation":')
        store = StateStore(self.root)
        self.assertEqual(store.current_generation, 2)

        # 用恢复后的 store 重新打开表，数据与定位完全可用
        table2 = Table(self.root)
        self.assertEqual(table2.locate(rid).rid, rid)
        table2.close()

    def test_empty_root_initializes_state(self):
        root = tempfile.mkdtemp(prefix="colstore-empty-")
        try:
            store = StateStore(root)
            self.assertEqual(store.current_generation, -1)
            self.assertTrue(os.path.exists(os.path.join(root, "state.json")))
        finally:
            shutil.rmtree(root, ignore_errors=True)

    def test_created_at_survives_restart_and_compaction(self):
        root = tempfile.mkdtemp(prefix="colstore-ts-")
        try:
            table = Table(root, schema=SCHEMA, table_name="t")
            table.create(generate_rows(40, seed=6), get_partitioner("by_region"))
            created_at = table.created_at
            self.assertTrue(created_at)
            table.close()

            # 模拟服务重启：从 manifest 恢复元信息
            table2 = Table(root)
            self.assertEqual(table2.created_at, created_at)
            table2.compact(make_shuffle(1), actor="t")
            table2.close()

            # 新一代 manifest 与 state 指针都保留创建时间，不回退为 None
            table3 = Table(root)
            self.assertEqual(table3.created_at, created_at)
            manifest = table3.store.read_manifest(table3.stats()["current_generation"])
            self.assertEqual(manifest["created_at"], created_at)
            with open(os.path.join(root, "state.json")) as fh:
                self.assertEqual(json.load(fh)["table"]["created_at"], created_at)
            table3.close()
        finally:
            shutil.rmtree(root, ignore_errors=True)

    def test_retention_quarantines_old_generations_but_keeps_logs(self):
        # keep=1 -> 窗口为当前代 + 1 个历史代；g0 在生成 g2 后被自动隔离
        self.assertEqual(self.table.store.available_generations(), [1, 2])
        orphans = self.table.store.list_orphans()
        self.assertTrue(any("gc-gen-000000" in name for name in orphans))
        # 再合并一次：g1 被隔离，窗口始终保持 2 代
        self.table.compact(make_shuffle(3), actor="t")
        self.assertEqual(self.table.store.available_generations(), [2, 3])
        # 日志永不清理，且完整保留了所有代的事件
        self.assertTrue(os.path.exists(os.path.join(self.root, "logs", "events.jsonl")))
        self.assertTrue(os.path.exists(os.path.join(self.root, "logs", "deltas.jsonl")))
        events = self.table.system_events(limit=500)
        self.assertTrue(any(e["type"] == "gc" for e in events))
        # 孤儿区里的旧代目录仍保留完整文件，可直接打开查证归属
        orphan = os.path.join(self.root, "_orphans")
        names = os.listdir(orphan)
        self.assertTrue(any("gc-gen-000000" in name for name in names))
        quarantined = os.path.join(orphan, names[0])
        self.assertTrue(os.path.exists(os.path.join(quarantined, "manifest.json")))


class TestIndexCost(unittest.TestCase):
    def test_index_is_fixed_width_and_lookup_is_logarithmic(self):
        root = tempfile.mkdtemp(prefix="colstore-idx-")
        try:
            table = Table(root, schema=SCHEMA, table_name="t")
            n = 600
            table.create(generate_rows(n, seed=4), get_partitioner("by_region"))
            gen = table.stats()["current_generation"]
            idx_path = os.path.join(table._paths(gen)["index"], "position.idx")
            reader = IndexReader(idx_path)
            # 定长记录：文件大小 = 头 + N * 112 字节，可直接算出成本
            expected_size = IDX_HEADER.size + n * IDX_RECORD_SIZE
            self.assertEqual(os.path.getsize(idx_path), expected_size)
            self.assertEqual(reader.count, n)

            # 每次合并重写全量索引（O(N) 维护成本），读取定位是二分 O(log N)
            entries = reader.all()
            sample = entries[:: max(1, n // 10)]
            t0 = time.perf_counter()
            for e in sample:
                got = reader.lookup(e.rid)
                self.assertEqual(got.ordinal, e.ordinal)
            elapsed = time.perf_counter() - t0
            self.assertLess(elapsed, 1.0)
            reader.close()
            table.close()
        finally:
            shutil.rmtree(root, ignore_errors=True)

    def test_index_bytes_scale_linearly_with_rows(self):
        sizes = []
        for n in (100, 200, 400):
            root = tempfile.mkdtemp(prefix=f"colstore-scale-{n}-")
            try:
                table = Table(root, schema=SCHEMA, table_name="t")
                table.create(generate_rows(n, seed=5), get_partitioner("by_region"))
                sizes.append((n, table.stats()["index_bytes"]))
                table.close()
            finally:
                shutil.rmtree(root, ignore_errors=True)
        # 每条 112 字节，索引体积随行数线性增长（容差内）
        (n1, b1), (n2, b2), (n3, b3) = sizes
        self.assertAlmostEqual(b2 / b1, n2 / n1, delta=0.05)
        self.assertAlmostEqual(b3 / b1, n3 / n1, delta=0.05)


if __name__ == "__main__":
    unittest.main(verbosity=2)
