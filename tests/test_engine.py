"""引擎端到端测试：定位不变性、OCC 冲突、删除、快照保留、切换失败恢复、并发。"""
import os
import shutil
import sys
import tempfile
import threading
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from colstore import DeltaConflict, NotFound, SnapshotExpired, Table
from colstore.demo_data import SCHEMA, generate_rows
from colstore.partitions import get_partitioner, make_shuffle


class EngineTestBase(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="colstore-test-")
        self.table = Table(self.root, schema=SCHEMA, table_name="t", keep_generations=3)
        self.rows = generate_rows(200, seed=1)
        self.table.create(self.rows, get_partitioner("by_region"), actor="test")

    def tearDown(self):
        self.table.close()
        shutil.rmtree(self.root, ignore_errors=True)


class TestLocateAcrossReorder(EngineTestBase):
    def test_rid_stable_through_repartition_and_shuffle(self):
        views = self.table.scan(limit=20)
        picked = [(v.rid, dict(v.row), v.location) for v in views[:5]]

        # 第一轮：改成 region+status 重分区
        self.table.compact(get_partitioner("by_region_status"), actor="t", mode="repartition")
        # 第二轮：哈希桶，彻底打散物理邻近
        self.table.compact(get_partitioner("by_bucket"), actor="t", mode="repartition")
        # 第三轮：随机洗牌，段内顺序全变
        self.table.compact(make_shuffle(seed=7), actor="t", mode="shuffle")

        for rid, original_row, old_loc in picked:
            view = self.table.locate(rid)
            self.assertFalse(view.deleted)
            self.assertEqual(view.row["order_id"], original_row["order_id"])
            self.assertEqual(view.row["customer"], original_row["customer"])
            # 物理位置必须已经变化，否则测试没有真正证明“不依赖行号”
            self.assertNotEqual(view.location, old_loc)
            self.assertEqual(view.generation, 3)

    def test_snapshot_link_pins_generation(self):
        view = self.table.scan(limit=1)[0]
        rid, original_status = view.rid, view.row["status"]

        # 在 g0 生命周期内更正一次
        self.table.correct(rid, {"status": "已签收"}, base_version=view.version, actor="A")
        seq_after_first = self.table.history(rid)[-1]["seq"]
        self.table.compact(get_partitioner("by_bucket"), actor="t")  # -> g1

        # g0 时间点快照（封顶到第一次更正的序号）必须重放该次更正
        snap0 = self.table.locate_snapshot(rid, 0, max_seq=seq_after_first)
        self.assertEqual(snap0.row["status"], "已签收")
        self.assertEqual(snap0.generation, 0)

        # 在 g1 再更正；带序号封顶的 g0 快照不被后代事件污染
        cur = self.table.locate(rid)
        self.table.correct(rid, {"status": "异常"}, base_version=cur.version, actor="B")
        self.assertEqual(self.table.locate(rid).row["status"], "异常")
        self.assertEqual(
            self.table.locate_snapshot(rid, 0, max_seq=seq_after_first).row["status"],
            "已签收")
        # g1 快照（不封顶，取 g1 切换时刻）看到最新值
        self.assertEqual(self.table.locate_snapshot(rid, 1).row["status"], "异常")

    def test_snapshot_seq_ceiling_recovers_older_value(self):
        view = self.table.scan(limit=1)[0]
        rid = view.rid
        v0 = view.version
        self.table.correct(rid, {"status": "运输中"}, base_version=v0, actor="A")
        seq1 = self.table.history(rid)[-1]["seq"]
        cur = self.table.locate(rid)
        self.table.correct(rid, {"status": "派送中"}, base_version=cur.version, actor="B")
        # 封顶在第一次更正前（seq < seq1）：快照回退到段里的原始值
        snap = self.table.locate_snapshot(rid, 0, max_seq=seq1 - 1)
        self.assertEqual(snap.row["status"], view.row["status"])
        # 封顶在第一次更正后：看到“运输中”，看不到之后的“派送中”
        snap2 = self.table.locate_snapshot(rid, 0, max_seq=seq1)
        self.assertEqual(snap2.row["status"], "运输中")

    def test_snapshot_preserves_delete_state_of_that_generation(self):
        view = self.table.scan(limit=1)[0]
        rid = view.rid
        # g0 内删除并合并到 g1：封顶到删除事件的 g0 快照应呈现删除态
        self.table.delete(rid, base_version=view.version, actor="A")
        delete_seq = self.table.history(rid)[-1]["seq"]
        self.table.compact(get_partitioner("by_bucket"), actor="t")
        self.assertTrue(
            self.table.locate_snapshot(rid, 0, max_seq=delete_seq).deleted)
        # 在 g1 复活后，封顶的 g0 快照仍然钉在删除态，证明快照不随后代操作变化
        self.table.undelete(rid, actor="admin")
        self.assertFalse(self.table.locate(rid).deleted)
        self.assertTrue(
            self.table.locate_snapshot(rid, 0, max_seq=delete_seq).deleted)
        self.assertFalse(self.table.locate_snapshot(rid, 1).deleted)

    def test_expired_snapshot_does_not_alias_another_record(self):
        rid = self.table.scan(limit=1)[0].rid
        # keep=3：当前代 + 3 个历史代。前 3 次合并 g0 仍在窗口内，第 4 次才过期
        for seed in (1, 2, 3):
            self.table.compact(make_shuffle(seed), actor="t")
        self.assertIn(0, self.table.stats()["available_generations"])
        self.table.compact(make_shuffle(4), actor="t")
        self.assertEqual(self.table.stats()["current_generation"], 4)
        self.assertNotIn(0, self.table.stats()["available_generations"])
        with self.assertRaises(SnapshotExpired):
            self.table.locate_snapshot(rid, 0)
        # 永久链接不受 GC 影响
        self.assertEqual(self.table.locate(rid).rid, rid)


class TestOCCCorrection(EngineTestBase):
    def test_stale_correction_rejected(self):
        view = self.table.scan(limit=1)[0]
        rid, v0 = view.rid, view.version

        ok = self.table.correct(rid, {"status": "派送中"}, base_version=v0, actor="A")
        self.assertEqual(ok.version, v0 + 1)
        # B 拿着旧版本号提交 -> 拒绝，A 的值不被覆盖
        with self.assertRaises(DeltaConflict) as ctx:
            self.table.correct(rid, {"status": "异常"}, base_version=v0, actor="B")
        self.assertEqual(ctx.exception.current, v0 + 1)
        self.assertEqual(self.table.locate(rid).row["status"], "派送中")

        # B 刷新到新版本后可以正常提交
        fresh = self.table.locate(rid)
        ok2 = self.table.correct(rid, {"status": "异常"}, base_version=fresh.version, actor="B")
        self.assertEqual(ok2.version, v0 + 2)
        self.assertEqual(self.table.locate(rid).row["status"], "异常")

    def test_history_records_every_version_and_actor(self):
        view = self.table.scan(limit=1)[0]
        rid = view.rid
        for actor, status in (("A", "运输中"), ("B", "派送中"), ("A", "已签收")):
            cur = self.table.locate(rid)
            self.table.correct(rid, {"status": status}, base_version=cur.version, actor=actor)
        history = self.table.history(rid)
        # 第 0 条是 insert，之后是三次 update；update 的版本号连续递增
        updates = [h for h in history if h["type"] == "update"]
        self.assertEqual([h["actor"] for h in updates], ["A", "B", "A"])
        self.assertEqual([h["new_version"] for h in history], [1, 2, 3, 4])
        self.assertEqual(updates[1]["old_values"]["status"], "运输中")


class TestDeleteAndUndelete(EngineTestBase):
    def test_delete_then_locate_shows_tombstone(self):
        view = self.table.scan(limit=1)[0]
        rid = view.rid
        self.table.delete(rid, base_version=view.version, actor="A")

        located = self.table.locate(rid)
        self.assertTrue(located.deleted)
        self.assertIsNone(located.row)
        # 扫描结果里不再出现
        self.assertNotIn(rid, [v.rid for v in self.table.scan()])
        # 历史链完整
        self.assertEqual(self.table.history(rid)[-1]["type"], "delete")

        # 复活后 RID 不变、版本继续递增、历史不断
        restored = self.table.undelete(rid, actor="admin")
        self.assertEqual(restored.rid, rid)
        self.assertFalse(restored.deleted)
        self.assertEqual(restored.version, view.version + 2)
        self.assertEqual(restored.row["order_id"], view.row["order_id"])

    def test_rid_never_reused_after_many_compactions(self):
        view = self.table.scan(limit=1)[0]
        rid = view.rid
        self.table.delete(rid, base_version=view.version, actor="A")
        for seed in range(5):
            self.table.compact(make_shuffle(seed), actor="t")
        current_gen = self.table.stats()["current_generation"]
        # 多代合并后墓碑仍在最新索引里：定位得到删除态，扫描不可见，RID 不丢
        snap = self.table.locate_snapshot(rid, current_gen)
        self.assertTrue(snap.deleted)
        located = self.table.locate(rid)
        self.assertTrue(located.deleted)
        self.assertNotIn(rid, [v.rid for v in self.table.scan()])
        # 追加新记录后，旧 RID 不会被复用
        self.table.ingest(
            [generate_rows(1, seed=999)[0]], get_partitioner("by_region"), actor="t"
        )
        active_rids = [v.rid for v in self.table.scan()]
        self.assertNotIn(rid, active_rids)


class TestSwapFailure(EngineTestBase):
    def test_failed_switch_keeps_old_generation_serving(self):
        rid = self.table.scan(limit=1)[0].rid
        old_gen = self.table.stats()["current_generation"]

        def fail_hook(generation):
            raise RuntimeError("模拟 manifest 切换失败")

        self.table.pre_swap_hook = fail_hook
        with self.assertRaises(RuntimeError):
            self.table.compact(get_partitioner("by_bucket"), actor="t")
        self.table.pre_swap_hook = None

        stats = self.table.stats()
        self.assertEqual(stats["current_generation"], old_gen)
        # 旧代仍可定位
        self.assertEqual(self.table.locate(rid).rid, rid)
        # 失败产物在孤儿区可查归属，事件日志有记录
        self.assertTrue(any("failed-swap" in o for o in stats["orphans"]))
        events = self.table.system_events()
        self.assertTrue(any(e["type"] == "switch_failed" for e in events))

        # 清理孤儿区后重试合并成功
        self.table.store.purge_orphans()
        new_gen = self.table.compact(get_partitioner("by_bucket"), actor="t")
        self.assertEqual(new_gen, old_gen + 1)
        self.assertEqual(self.table.locate(rid).rid, rid)


class TestConcurrency(EngineTestBase):
    def test_parallel_corrections_serialize_without_lost_update(self):
        view = self.table.scan(limit=1)[0]
        rid, v0 = view.rid, view.version
        errors = []

        def worker(actor):
            # 每个工作线程自旋重试：遇到冲突就刷新版本号再提交，
            # 直到自己的 5 次更新全部落地。最终成功更新必须恰为 20 次、无丢失。
            done = 0
            while done < 5:
                cur = self.table.locate(rid)
                try:
                    self.table.correct(
                        rid, {"amount": cur.row["amount"] + 1},
                        base_version=cur.version, actor=actor,
                    )
                    done += 1
                except DeltaConflict as exc:
                    errors.append(exc)

        threads = [threading.Thread(target=worker, args=(f"U{i}",)) for i in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        final = self.table.locate(rid)
        history = [h for h in self.table.history(rid) if h["type"] == "update"]
        self.assertEqual(len(history), 20)                 # 没有更新丢失
        self.assertEqual(final.version, v0 + 20)
        self.assertEqual(final.row["amount"], view.row["amount"] + 20)
        # 高并发下确实观察到了冲突拒绝（证明 OCC 在起作用）
        self.assertGreater(len(errors), 0)

    def test_reads_run_while_compaction_holds_write_lock(self):
        rids = [v.rid for v in self.table.scan(limit=50)]
        seen_during_compact = []

        def reader():
            for rid in rids:
                seen_during_compact.append(self.table.locate(rid).rid)

        def compactor():
            for seed in range(3):
                self.table.compact(make_shuffle(seed + 10), actor="bg-merge")

        t1 = threading.Thread(target=reader)
        t2 = threading.Thread(target=compactor)
        t1.start()
        t2.start()
        t1.join()
        t2.join()
        self.assertEqual(sorted(seen_during_compact), sorted(rids))
        self.assertEqual(self.table.stats()["current_generation"], 3)


class TestColumnarStorage(unittest.TestCase):
    def test_projection_only_reads_requested_columns(self):
        root = tempfile.mkdtemp(prefix="colstore-col-")
        try:
            table = Table(root, schema=SCHEMA, table_name="t")
            table.create(generate_rows(100, seed=2), get_partitioner("by_region"))
            gen = table.stats()["current_generation"]
            reader = table._segment_reader(gen, table.stats()["segments"][0]["name"])
            reader.read_rows(SCHEMA, columns=["order_id"])
            self.assertEqual(set(reader._cache), {"order_id"})
            full = reader.read_rows(SCHEMA)
            self.assertEqual(set(full[0].keys()), set(SCHEMA))
        finally:
            table.close()
            shutil.rmtree(root, ignore_errors=True)

    def test_null_and_type_roundtrip(self):
        from colstore.encoding import decode_values, encode_values
        for dtype, values in (
            ("int64", [1, None, -7, 0]),
            ("float64", [1.5, None, -0.25]),
            ("bool", [True, False, None, True]),
            ("str", ["a", None, "中文运单", ""]),
        ):
            blob = encode_values(values, dtype)
            self.assertEqual(decode_values(blob, dtype), values)


if __name__ == "__main__":
    unittest.main(verbosity=2)
