"""追加式日志：系统事件日志 + 记录级更正流水（delta log）。

两条日志都是只追加（append-only）、每行一个 JSON、每条 fsync：
- events.jsonl：表级事件（摄入/合并/切换/GC/失败），永久保留，用于审计
  「这条记录曾经归属于哪个段、哪次合并动过它」。
- deltas.jsonl：记录级变更（插入/更正/删除），携带 base_version 做乐观
  并发控制。文件不按代切割，事件里带 base_generation，旧快照读取时只
  重放属于该代的事件。
"""
from __future__ import annotations

import json
import os
import threading
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional


def _now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime())


class AppendLog:
    def __init__(self, path: str):
        self.path = path
        self._lock = threading.Lock()
        os.makedirs(os.path.dirname(path), exist_ok=True)

    def append(self, record: Dict[str, Any]) -> Dict[str, Any]:
        line = json.dumps(record, ensure_ascii=False, sort_keys=True)
        with self._lock:
            with open(self.path, "a", encoding="utf-8") as fh:
                fh.write(line + "\n")
                fh.flush()
                os.fsync(fh.fileno())
        return record

    def read_all(self) -> List[Dict[str, Any]]:
        if not os.path.exists(self.path):
            return []
        with open(self.path, "r", encoding="utf-8") as fh:
            return [json.loads(line) for line in fh if line.strip()]


class EventLog(AppendLog):
    """表级审计事件，序号全局递增。"""

    def __init__(self, path: str):
        super().__init__(path)
        self._seq = len(self.read_all())

    def emit(self, event_type: str, actor: str = "system", **detail: Any) -> Dict[str, Any]:
        with self._lock:
            seq = self._seq
            self._seq += 1
        return self.append(
            {"seq": seq, "ts": _now(), "type": event_type, "actor": actor, "detail": detail}
        )


@dataclass
class DeltaConflict(Exception):
    """过期更正：base_version 与当前版本不一致。"""

    rid: str
    supplied: int
    current: int

    def __str__(self) -> str:
        return (
            f"记录 {self.rid} 的更正基于第 {self.supplied} 版，"
            f"但当前已是第 {self.current} 版，拒绝覆盖（请刷新后重新合并）"
        )


class DeltaLog(AppendLog):
    """记录级变更流水，维护每个 RID 的当前版本。"""

    def __init__(self, path: str):
        super().__init__(path)
        self._seq = 0
        self._versions: Dict[str, int] = {}
        for event in self.read_all():
            self._seq = max(self._seq, event["seq"] + 1)
            self._versions[event["rid"]] = event["new_version"]

    def current_version(self, rid: str) -> int:
        return self._versions.get(rid, 0)

    @property
    def next_seq(self) -> int:
        return self._seq

    def append_event(
        self,
        *,
        rid: str,
        event_type: str,
        actor: str,
        base_version: Optional[int],
        row: Optional[Dict[str, Any]],
        old_values: Optional[Dict[str, Any]],
        base_generation: int,
    ) -> Dict[str, Any]:
        with self._lock:
            current = self._versions.get(rid, 0)
            if event_type == "insert":
                if rid in self._versions:
                    raise DeltaConflict(rid, base_version or 0, current)
                new_version = 1
            else:
                if base_version is None or base_version != current:
                    raise DeltaConflict(rid, base_version if base_version is not None else -1, current)
                new_version = current + 1
            event = {
                "seq": self._seq,
                "ts": _now(),
                "rid": rid,
                "type": event_type,
                "actor": actor,
                "base_version": base_version,
                "new_version": new_version,
                "base_generation": base_generation,
                "row": row,
                "old_values": old_values,
            }
            self._seq += 1
            self._versions[rid] = new_version
        # 持久化在锁外做（AppendLog 自带锁）；版本号已在上面的锁内敲定
        return self.append(event)

    def history_for(self, rid: str) -> List[Dict[str, Any]]:
        return [e for e in self.read_all() if e["rid"] == rid]

    def latest_events(self) -> Dict[str, Dict[str, Any]]:
        """每个 RID 的最新事件，构建当前态覆盖层用。"""
        latest: Dict[str, Dict[str, Any]] = {}
        for event in self.read_all():
            latest[event["rid"]] = event
        return latest
