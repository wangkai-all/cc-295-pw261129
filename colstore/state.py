"""表状态管理：manifest 版本链、当前代指针、崩溃恢复、历史保留与 GC。

目录布局（每张表一个根目录）
============================
  state.json                 当前指针（原子写，写坏可从 manifest 链重建）
  lock.file                  跨进程排他锁
  logs/events.jsonl          系统事件日志（永久保留）
  logs/deltas.jsonl          记录级变更流水（永久保留）
  gen-000000/
    manifest.json            该代段/索引清单 + 统计
    data/<seg>.col           列存段
    index/<seg>.idx          定位索引侧车
  gen-000001/ ...
  staged-gen-N/              合并产出的暂存目录，切换成功才转正
  _orphans/                  切换/GC 失败的遗留物，统一收拢到这里便于查清归属
"""
from __future__ import annotations

import glob
import json
import os
import shutil
from typing import Any, Dict, List, Optional

from .locking import FileLock

STATE_FILE = "state.json"
LOCK_FILE = "lock.file"
LOGS_DIR = "logs"


def gen_dir(root: str, generation: int) -> str:
    return os.path.join(root, f"gen-{generation:06d}")


def staged_dir(root: str, generation: int) -> str:
    return os.path.join(root, f"staged-gen-{generation:06d}")


class StateError(Exception):
    pass


class StateStore:
    def __init__(self, root: str):
        self.root = os.path.abspath(root)
        self.lock_path = os.path.join(self.root, LOCK_FILE)
        os.makedirs(os.path.join(self.root, LOGS_DIR), exist_ok=True)
        self._state = self._load_or_recover()

    # ---------- 基础读写 ----------

    def _atomic_write_json(self, path: str, payload: Dict[str, Any]) -> None:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, ensure_ascii=False, indent=2, sort_keys=True)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, path)

    def _read_json(self, path: str) -> Dict[str, Any]:
        with open(path, "r", encoding="utf-8") as fh:
            return json.load(fh)

    def _load_or_recover(self) -> Dict[str, Any]:
        path = os.path.join(self.root, STATE_FILE)
        if os.path.exists(path):
            try:
                return self._read_json(path)
            except json.JSONDecodeError:
                pass  # 写入中途崩溃，落到下面的重建
        manifests = sorted(glob.glob(os.path.join(self.root, "gen-*", "manifest.json")))
        if not manifests:
            state = {"current_generation": -1, "table": None}
            self._atomic_write_json(path, state)
            return state
        latest = self._read_json(manifests[-1])
        state = {
            "current_generation": latest["generation"],
            "table": latest.get("table"),
            "recovered": True,
        }
        self._atomic_write_json(path, state)
        return state

    # ---------- 锁 ----------

    def file_lock(self) -> FileLock:
        return FileLock(self.lock_path)

    # ---------- 代与 manifest ----------

    @property
    def current_generation(self) -> int:
        return self._state["current_generation"]

    def next_generation(self) -> int:
        return self.current_generation + 1

    def manifest_path(self, generation: int) -> str:
        return os.path.join(gen_dir(self.root, generation), "manifest.json")

    def write_manifest(self, generation: int, manifest: Dict[str, Any]) -> None:
        self._atomic_write_json(self.manifest_path(generation), manifest)

    def read_manifest(self, generation: int) -> Optional[Dict[str, Any]]:
        path = self.manifest_path(generation)
        if not os.path.exists(path):
            return None
        return self._read_json(path)

    def available_generations(self) -> List[int]:
        out = []
        for path in sorted(glob.glob(os.path.join(self.root, "gen-*", "manifest.json"))):
            out.append(int(os.path.basename(os.path.dirname(path)).split("-")[1]))
        return out

    def stage_paths(self, generation: int) -> Dict[str, str]:
        base = staged_dir(self.root, generation)
        return {
            "base": base,
            "data": os.path.join(base, "data"),
            "index": os.path.join(base, "index"),
            "manifest": os.path.join(base, "manifest.json"),
        }

    # ---------- 原子切换 ----------

    def switch_generation(self, generation: int, table_meta: Dict[str, Any]) -> None:
        """把暂存目录转正并推进当前指针。任何一步失败都不破坏旧代。"""
        staged = staged_dir(self.root, generation)
        target = gen_dir(self.root, generation)
        if not os.path.exists(os.path.join(staged, "manifest.json")):
            raise StateError(f"暂存清单不存在，无法切换到第 {generation} 代: {staged}")
        with self.file_lock():
            if os.path.exists(target):
                # 极少数重试场景：目标已存在，先移入孤儿区而不是直接删
                self.quarantine(target, f"pre-existing-gen-{generation:06d}")
            os.replace(staged, target)
            new_state = {
                "current_generation": generation,
                "table": table_meta,
            }
            self._atomic_write_json(os.path.join(self.root, STATE_FILE), new_state)
            self._state = new_state

    def quarantine(self, path: str, reason: str) -> str:
        """切换失败/异常遗留统一收拢到 _orphans/，归属可查、不静默删除。"""
        orphans = os.path.join(self.root, "_orphans")
        os.makedirs(orphans, exist_ok=True)
        name = f"{reason}-{os.path.basename(path.rstrip('/'))}"
        dest = os.path.join(orphans, name)
        if os.path.exists(path):
            os.replace(path, dest)
        return dest

    def list_orphans(self) -> List[str]:
        orphans = os.path.join(self.root, "_orphans")
        if not os.path.exists(orphans):
            return []
        return sorted(os.listdir(orphans))

    def list_staged(self) -> List[str]:
        return [
            os.path.basename(p)
            for p in sorted(glob.glob(os.path.join(self.root, "staged-gen-*")))
        ]

    # ---------- 历史保留与 GC ----------

    def enforce_retention(self, keep_generations: int) -> List[str]:
        """保留当前代 + 最近 keep_generations 个历史代；日志永不清理。

        例：keep=3、当前代=g5 时保留 g2/g3/g4/g5，g0/g1 移入 _orphans。
        返回被清理的代目录（收拢而非删除，可回查后手工 purge）。
        """
        gens = self.available_generations()
        kept = set(gens[-(keep_generations + 1):])
        expired = [g for g in gens if g not in kept]
        removed = []
        for g in expired:
            dest = self.quarantine(gen_dir(self.root, g), f"gc-gen-{g:06d}")
            removed.append(dest)
        return removed

    def purge_orphans(self) -> None:
        orphans = os.path.join(self.root, "_orphans")
        if os.path.exists(orphans):
            shutil.rmtree(orphans)
