"""表引擎：列存段 + 定位索引 + delta 覆盖层 + 乐观并发 + 并发合并。

链接语义
========
- 永久链接  /#/record/<rid>                永远指向同一逻辑记录，不随物理位置变化
- 快照链接  /#/snapshot/<gen>/record/<rid> 固定在第 gen 代的视图；超保留期显式报错，
                                           绝不会静默落到另一条记录上

并发模型（单进程多线程）
=======================
- RWLock 读锁：浏览、定位、快照读取，可并发
- RWLock 写锁：摄入、更正、删除、合并切换，独占
- DeltaLog 内部锁保证版本号分配原子；更正采用乐观并发控制（OCC），
  提交时校验 base_version == 当前版本，过期即拒，不覆盖他人新值
"""
from __future__ import annotations

import json
import os
import threading
import time
import uuid
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Tuple

from .index import IndexEntry, IndexReader, IndexWriter
from .journal import DeltaConflict, DeltaLog, EventLog
from .locking import RWLock
from .segment import SegmentReader, SegmentWriter
from .state import StateStore, gen_dir

RID_BYTES = 32


def new_rid() -> str:
    return uuid.uuid4().hex


@dataclass
class RecordView:
    rid: str
    row: Optional[Dict[str, Any]]
    version: int
    deleted: bool
    generation: int
    location: Optional[Dict[str, Any]] = None
    conflict: Optional[DeltaConflict] = None


class NotFound(Exception):
    pass


class SnapshotExpired(Exception):
    def __init__(self, generation: int, available: List[int]):
        self.generation = generation
        self.available = available
        super().__init__(
            f"第 {generation} 代快照已超出历史保留范围（现存代：{available or '无'}），"
            "该链接不会重定向到其他记录"
        )


class Table:
    def __init__(self, root: str, schema: Optional[Dict[str, str]] = None,
                 table_name: str = "shipments", keep_generations: int = 3):
        self.root = os.path.abspath(root)
        self.store = StateStore(self.root)
        self.events = EventLog(os.path.join(self.root, "logs", "events.jsonl"))
        self.deltas = DeltaLog(os.path.join(self.root, "logs", "deltas.jsonl"))
        self.lock = RWLock()
        self._seg_cache: Dict[str, SegmentReader] = {}
        self._idx_cache: Dict[int, IndexReader] = {}
        self.keep_generations = keep_generations
        # 测试/演示钩子：在暂存完成后、切换前抛异常，模拟切换失败
        self.pre_swap_hook: Optional[Callable[[int], None]] = None

        if self.store.current_generation < 0:
            if schema is None:
                raise NotFound(f"表不存在且未提供 schema: {self.root}")
            self.schema = dict(schema)
            self.table_name = table_name
            self.created_at = None
        else:
            manifest = self.store.read_manifest(self.store.current_generation)
            self.schema = dict(manifest["schema"])
            self.table_name = manifest.get("table", table_name)
            self.created_at = manifest.get("created_at")

    # ================= 路径辅助 =================

    def _paths(self, generation: int) -> Dict[str, str]:
        base = gen_dir(self.root, generation)
        return {
            "base": base,
            "data": os.path.join(base, "data"),
            "index": os.path.join(base, "index"),
            "manifest": os.path.join(base, "manifest.json"),
        }

    def _index_reader(self, generation: int) -> IndexReader:
        if generation not in self._idx_cache:
            paths = self._paths(generation)
            idx_path = os.path.join(paths["index"], "position.idx")
            self._idx_cache[generation] = IndexReader(idx_path)
        return self._idx_cache[generation]

    def _segment_reader(self, generation: int, segment: str) -> SegmentReader:
        key = f"{generation}:{segment}"
        if key not in self._seg_cache:
            path = os.path.join(self._paths(generation)["data"], segment + ".col")
            self._seg_cache[key] = SegmentReader(path)
        return self._seg_cache[key]

    def _drop_caches(self) -> None:
        for reader in self._idx_cache.values():
            reader.close()
        self._idx_cache.clear()
        self._seg_cache.clear()

    # ================= 首次摄入建表 =================

    def create(self, rows: List[Dict[str, Any]], partition_fn: Callable[[Dict[str, Any]], str],
               actor: str = "system") -> int:
        with self.lock.write_lock():
            if self.store.current_generation >= 0:
                raise RuntimeError("表已存在，请用 ingest 追加数据")
            generation = 0
            self.created_at = time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime())
            self._build_generation(
                generation,
                rows,
                partition_fn=partition_fn,
                mode="create",
                actor=actor,
                assign_rids=True,
                base_generation=0,
            )
            # 建表行同样进入 delta 流水：版本从 1 起、历史链与复活逻辑统一
            for row in rows:
                self.deltas.append_event(
                    rid=row["__rid__"], event_type="insert", actor=actor,
                    base_version=None,
                    row={k: v for k, v in row.items() if not k.startswith("__")},
                    old_values=None, base_generation=generation,
                )
            self.events.emit(
                "create", actor=actor, generation=generation,
                rows=len(rows), partitioner=getattr(partition_fn, "__name__", "partition_fn"),
            )
            self._gc_locked(actor)
            return generation

    # ================= 追加摄入（新一代） =================

    def ingest(self, new_rows: List[Dict[str, Any]],
               partition_fn: Callable[[Dict[str, Any]], str],
               actor: str = "system") -> int:
        with self.lock.write_lock():
            current = self.store.current_generation
            generation = current + 1
            existing = self._materialize_current_locked()
            new_rids = []
            for row in new_rows:
                row["__rid__"] = new_rid()
                row["__version__"] = 1
                row["__deleted__"] = False
                new_rids.append(row["__rid__"])
            all_rows = existing + new_rows
            self._build_generation(
                generation, all_rows, partition_fn=partition_fn,
                mode="ingest", actor=actor, assign_rids=False, base_generation=generation,
            )
            # 为新行补 insert 事件（base_generation 记为新一代）
            for row in new_rows:
                self.deltas.append_event(
                    rid=row["__rid__"], event_type="insert", actor=actor,
                    base_version=None,
                    row={k: v for k, v in row.items() if not k.startswith("__")},
                    old_values=None, base_generation=generation,
                )
            self.events.emit(
                "ingest", actor=actor, generation=generation,
                new_rows=len(new_rows), total=len(all_rows),
            )
            self._gc_locked(actor)
            return generation

    # ================= 代构建（内部） =================

    def _build_generation(self, generation: int, rows: List[Dict[str, Any]], *,
                          partition_fn: Callable[[Dict[str, Any]], str],
                          mode: str, actor: str, assign_rids: bool,
                          base_generation: int) -> None:
        staged = self.store.stage_paths(generation)
        os.makedirs(staged["data"], exist_ok=True)
        os.makedirs(staged["index"], exist_ok=True)

        partitions: Dict[str, List[Dict[str, Any]]] = {}
        for row in rows:
            if assign_rids:
                row["__rid__"] = new_rid()
                row["__version__"] = 1
                row["__deleted__"] = False
            key = _safe_partition(partition_fn(row))
            partitions.setdefault(key, []).append(row)

        manifest_segments: List[Dict[str, Any]] = []
        index_writer = IndexWriter(os.path.join(staged["index"], "position.idx"), generation)
        total_rows = 0
        bytes_written = 0
        for part_index, part in enumerate(sorted(partitions)):
            part_rows = partitions[part]
            # 索引侧车的段字段是定长 ASCII，段 ID 统一用序号安全名；
            # 人类可读的分区键（可能含中文）只保存在 manifest 里
            seg_name = f"seg-{part_index:04d}"
            seg_path = os.path.join(staged["data"], seg_name + ".col")
            writer = SegmentWriter(seg_path, self.schema)
            data_ordinal = 0
            for row in part_rows:
                is_deleted = row.get("__deleted__", False)
                index_writer.add(
                    row["__rid__"], seg_name,
                    # 墓碑不进段数据，ordinal 指向同段最后一条数据行之后的位置
                    data_ordinal if not is_deleted else max(data_ordinal - 1, 0),
                    version=row.get("__version__", 1),
                    deleted=is_deleted,
                )
                if not is_deleted:
                    writer.add_row({k: row.get(k) for k in self.schema})
                    data_ordinal += 1
            writer.finish()
            bytes_written += os.path.getsize(seg_path)
            manifest_segments.append({
                "name": seg_name, "partition": part,
                "rows": len(part_rows), "bytes": os.path.getsize(seg_path),
            })
            total_rows += len(part_rows)
        index_writer.finish()
        index_bytes = os.path.getsize(os.path.join(staged["index"], "position.idx"))

        manifest = {
            "table": self.table_name,
            "created_at": self.created_at,
            "generation": generation,
            "schema": self.schema,
            "mode": mode,
            "created_by": actor,
            "partitioner": getattr(partition_fn, "__name__", "partition_fn"),
            "segments": manifest_segments,
            "row_count": total_rows,
            "index_bytes": index_bytes,
            "data_bytes": bytes_written,
        }
        self.store._atomic_write_json(staged["manifest"], manifest)
        self._swap(generation, manifest, actor)

    def _swap(self, generation: int, manifest: Dict[str, Any], actor: str) -> None:
        """暂存 -> 原子切换。钩子抛异常时暂存目录进孤儿区，旧代继续服务。"""
        if self.pre_swap_hook is not None:
            try:
                self.pre_swap_hook(generation)
            except Exception as exc:
                dest = self.store.quarantine(
                    self.store.stage_paths(generation)["base"],
                    f"failed-swap-gen-{generation:06d}",
                )
                self.events.emit(
                    "switch_failed", actor=actor, generation=generation,
                    error=str(exc), quarantined=dest,
                )
                raise
        table_meta = {"table": self.table_name, "created_at": self.created_at}
        self.store.switch_generation(generation, table_meta)
        self._drop_caches()
        self.events.emit(
            "switch", actor=actor, generation=generation,
            segments=len(manifest["segments"]), rows=manifest["row_count"],
            index_bytes=manifest["index_bytes"], data_bytes=manifest["data_bytes"],
        )

    # ================= 当前态物化与覆盖层 =================

    def _overlay(self) -> Dict[str, Dict[str, Any]]:
        return self.deltas.latest_events()

    def _materialize_current_locked(self, include_tombstones: bool = False) -> List[Dict[str, Any]]:
        """把当前代段数据 + delta 覆盖层物化成内部行（带 __rid__ 等）。

        include_tombstones=True（合并场景）时保留删除墓碑：墓碑不进段数据，
        但以 __deleted__ 标记进入新一代索引，保证删除后仍可定位、RID 不丢。
        """
        gen = self.store.current_generation
        if gen < 0:
            return []
        index = self._index_reader(gen)
        overlay = self._overlay()
        out: List[Dict[str, Any]] = []
        for entry in index.all():
            if entry.rid in overlay:
                event = overlay[entry.rid]
                if event["type"] == "delete":
                    if include_tombstones:
                        out.append({
                            "__rid__": entry.rid,
                            "__version__": event["new_version"],
                            "__deleted__": True,
                        })
                    continue
                row = dict(event["row"])
                row["__rid__"] = entry.rid
                row["__version__"] = event["new_version"]
                row["__deleted__"] = False
            else:
                reader = self._segment_reader(gen, entry.segment)
                row = reader.read_row(self.schema, entry.ordinal)
                row["__rid__"] = entry.rid
                row["__version__"] = entry.version
                row["__deleted__"] = entry.deleted
                if entry.deleted:
                    if include_tombstones:
                        out.append(row)
                    continue
            out.append(row)
        return out

    # ================= 浏览（读锁，可并发） =================

    def scan(self, columns: Optional[List[str]] = None, where: Optional[Dict[str, Any]] = None,
             limit: Optional[int] = None) -> List[RecordView]:
        with self.lock.read_lock():
            gen = self.store.current_generation
            index = self._index_reader(gen)
            overlay = self._overlay()
            views: List[RecordView] = []
            for entry in index.all():
                view = self._view_from_entry_locked(entry, gen, overlay)
                if view.deleted:
                    continue
                if where and not _matches(view.row, where):
                    continue
                views.append(view)
                if limit and len(views) >= limit:
                    break
            return views

    def locate(self, rid: str) -> RecordView:
        """永久链接：始终按 RID 定位当前态。"""
        with self.lock.read_lock():
            gen = self.store.current_generation
            return self._locate_locked(rid, gen, allow_history_fallback=True)

    def locate_snapshot(self, rid: str, generation: int,
                        max_seq: Optional[int] = None) -> RecordView:
        """时间点快照：固定第 generation 代的物理布局，delta 只重放到 max_seq。

        - max_seq=None：取该代切换时刻（最后一个 base_generation<=generation 事件）的序号；
        - 快照只反映该序号之前发生的更正，之后的任何操作都不污染它；
        - 代超出保留期显式抛 SnapshotExpired，绝不静默指向另一条记录。
        """
        with self.lock.read_lock():
            available = self.store.available_generations()
            if generation not in available:
                raise SnapshotExpired(generation, available)
            if max_seq is None:
                prior = [e["seq"] for e in self.deltas.read_all()
                         if e["base_generation"] <= generation]
                max_seq = max(prior, default=-1)
            return self._locate_locked(rid, generation, allow_history_fallback=False,
                                       snapshot=True, max_seq=max_seq)

    def _locate_locked(self, rid: str, generation: int, *,
                       allow_history_fallback: bool, snapshot: bool = False,
                       max_seq: Optional[int] = None) -> RecordView:
        index = self._index_reader(generation)
        entry = index.lookup(rid)
        overlay = self._overlay()
        if entry is not None:
            return self._view_from_entry_locked(
                entry, generation, overlay, snapshot=snapshot, max_seq=max_seq)
        # 代内索引没有：可能是该代之后新建/复活的记录
        event = overlay.get(rid)
        if event is not None and not snapshot:
            if event["type"] == "delete":
                return RecordView(rid, None, event["new_version"], True, generation)
            row = dict(event["row"])
            return RecordView(rid, row, event["new_version"], False, generation)
        if allow_history_fallback:
            history = self.deltas.history_for(rid)
            if history:
                last = history[-1]
                return RecordView(
                    rid,
                    None if last["type"] == "delete" else dict(last["row"] or {}),
                    last["new_version"],
                    last["type"] == "delete",
                    generation,
                )
        raise NotFound(f"RID {rid} 在第 {generation} 代中不存在")

    def _view_from_entry_locked(self, entry: IndexEntry, generation: int,
                                overlay: Dict[str, Dict[str, Any]],
                                snapshot: bool = False,
                                max_seq: Optional[int] = None) -> RecordView:
        location = {"segment": entry.segment, "ordinal": entry.ordinal,
                    "generation": generation}
        event = overlay.get(entry.rid)
        if snapshot:
            # 时间点快照：只重放 seq <= max_seq 的该 RID 事件。
            # 这样即使物理代还在保留期内，快照时刻之后的更正/删除也不会污染它。
            seq_events = [
                e for e in self.deltas.history_for(entry.rid)
                if (max_seq is None or e["seq"] <= max_seq)
            ]
            event = seq_events[-1] if seq_events else None
            if event is None:
                if entry.deleted:
                    return RecordView(entry.rid, None, entry.version, True,
                                      generation, location)
                reader = self._segment_reader(generation, entry.segment)
                row = reader.read_row(self.schema, entry.ordinal)
                return RecordView(entry.rid, row, entry.version, entry.deleted,
                                  generation, location)
        if event is not None:
            if event["type"] == "delete":
                return RecordView(entry.rid, None, event["new_version"], True,
                                  generation, location)
            row = dict(event["row"])
            return RecordView(entry.rid, row, event["new_version"], False,
                              generation, location)
        reader = self._segment_reader(generation, entry.segment)
        row = reader.read_row(self.schema, entry.ordinal)
        return RecordView(entry.rid, row, entry.version, entry.deleted,
                          generation, location)

    # ================= 更正（OCC，写锁） =================

    def correct(self, rid: str, changes: Dict[str, Any], base_version: int,
                actor: str) -> RecordView:
        with self.lock.write_lock():
            view = self._locate_locked(rid, self.store.current_generation,
                                       allow_history_fallback=True)
            if view.deleted:
                raise NotFound(f"记录 {rid} 已删除，不能更正（如需恢复请先复活）")
            current_row = dict(view.row)
            old_values = {k: current_row.get(k) for k in changes}
            current_row.update(changes)
            event = self.deltas.append_event(
                rid=rid, event_type="update", actor=actor,
                base_version=base_version, row=current_row,
                old_values=old_values,
                base_generation=self.store.current_generation,
            )
            self.events.emit(
                "correct", actor=actor, rid=rid,
                base_version=base_version, new_version=event["new_version"],
                changed=list(changes.keys()),
            )
            return RecordView(rid, current_row, event["new_version"], False,
                              self.store.current_generation, view.location)

    def delete(self, rid: str, base_version: int, actor: str) -> RecordView:
        with self.lock.write_lock():
            view = self._locate_locked(rid, self.store.current_generation,
                                       allow_history_fallback=True)
            if view.deleted:
                return view
            event = self.deltas.append_event(
                rid=rid, event_type="delete", actor=actor,
                base_version=base_version, row=view.row, old_values=None,
                base_generation=self.store.current_generation,
            )
            self.events.emit(
                "delete", actor=actor, rid=rid, base_version=base_version,
                new_version=event["new_version"],
            )
            return RecordView(rid, None, event["new_version"], True,
                              self.store.current_generation, view.location)

    def undelete(self, rid: str, actor: str) -> RecordView:
        """复活已删除记录：RID 复用同一标识，历史链不断。"""
        with self.lock.write_lock():
            history = self.deltas.history_for(rid)
            if not history or history[-1]["type"] != "delete":
                raise NotFound(f"记录 {rid} 未处于删除状态")
            insert = next((e for e in history if e["type"] == "insert"), None)
            last_row_event = next((e for e in reversed(history)
                                   if e["type"] in ("insert", "update") and e.get("row")), None)
            source = insert or last_row_event
            row = dict(source["row"]) if source else {}
            current_version = self.deltas.current_version(rid)
            event = self.deltas.append_event(
                rid=rid, event_type="update", actor=actor,
                base_version=current_version, row=row, old_values=None,
                base_generation=self.store.current_generation,
            )
            self.events.emit("undelete", actor=actor, rid=rid,
                             new_version=event["new_version"])
            return RecordView(rid, row, event["new_version"], False,
                              self.store.current_generation)

    # ================= 合并 / 重排 / 重分区 =================

    def compact(self, partition_fn: Callable[[Dict[str, Any]], str],
                actor: str = "system", mode: str = "compact") -> int:
        """独占写锁下重写全部数据：新段 + 新索引 + 原子切换。

        合并期间到达的更正会阻塞在写锁上；切换完成后它们校验的是新版本号，
        因此不会基于旧代覆盖他人新值。
        """
        with self.lock.write_lock():
            current = self.store.current_generation
            generation = current + 1
            rows = self._materialize_current_locked(include_tombstones=True)
            before = self.store.read_manifest(current)
            self._build_generation(
                generation, rows, partition_fn=partition_fn,
                mode=mode, actor=actor, assign_rids=False,
                base_generation=generation,
            )
            after = self.store.read_manifest(generation)
            self.events.emit(
                mode, actor=actor, from_generation=current,
                to_generation=generation,
                segments_before=len(before["segments"]),
                segments_after=len(after["segments"]),
                rows=after["row_count"],
                write_amplification=round(
                    after["data_bytes"] / max(before["data_bytes"], 1), 3),
            )
            self._gc_locked(actor)
            return generation

    def _gc_locked(self, actor: str) -> None:
        removed = self.store.enforce_retention(self.keep_generations)
        if removed:
            self.events.emit("gc", actor=actor, quarantined=removed,
                             keep_generations=self.keep_generations)

    # ================= 元信息与历史 =================

    def stats(self) -> Dict[str, Any]:
        with self.lock.read_lock():
            gen = self.store.current_generation
            manifest = self.store.read_manifest(gen) if gen >= 0 else None
            idx_entries = 0
            live = deleted = 0
            if manifest:
                idx_entries = self._index_reader(gen).count
                overlay = self._overlay()
                for entry in self._index_reader(gen).all():
                    ev = overlay.get(entry.rid)
                    is_del = ev["type"] == "delete" if ev else entry.deleted
                    deleted += 1 if is_del else 0
                live = idx_entries - deleted
            return {
                "table": self.table_name,
                "current_generation": gen,
                "available_generations": self.store.available_generations(),
                "keep_generations": self.keep_generations,
                "schema": self.schema,
                "row_count": manifest["row_count"] if manifest else 0,
                "live_records": live,
                "deleted_in_index": deleted,
                "index_entries": idx_entries,
                "index_bytes": manifest["index_bytes"] if manifest else 0,
                "data_bytes": manifest["data_bytes"] if manifest else 0,
                "segments": manifest["segments"] if manifest else [],
                "partitioner": manifest["partitioner"] if manifest else None,
                "orphans": self.store.list_orphans(),
                "staged": self.store.list_staged(),
                "delta_events": len(self.deltas.read_all()),
            }

    def history(self, rid: str) -> List[Dict[str, Any]]:
        with self.lock.read_lock():
            return self.deltas.history_for(rid)

    def system_events(self, limit: int = 50) -> List[Dict[str, Any]]:
        with self.lock.read_lock():
            return self.events.read_all()[-limit:]

    def close(self) -> None:
        """关闭索引 mmap 与文件句柄。"""
        with self.lock.write_lock():
            self._drop_caches()


def _safe_partition(key: str) -> str:
    cleaned = "".join(c if c.isalnum() or c in ("-", "_") else "_" for c in str(key))
    return cleaned[:48] or "default"


def _matches(row: Dict[str, Any], where: Dict[str, Any]) -> bool:
    return all(str(row.get(k, "")) == str(v) for k, v in where.items())
