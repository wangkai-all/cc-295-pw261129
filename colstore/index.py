"""定位索引：永久逻辑标识（RID）到物理位置的映射侧车文件。

设计要点
========
1. RID 是 32 字符 uuid4 hex，记录创建即分配，**永不复用**。
2. 索引文件与段文件同代（generation），每条定长记录、按 RID 排序，
   可用 mmap 做二分查找，不必整体载入内存。
3. 删除只是把 flags 的 deleted 位置 1（软删除墓碑），RID 与映射仍在，
   因此「删除后定位」仍能查到归属，而不是悄悄落到另一条记录上。
4. 每次合并/重排/摄入产生新一代，重写全量索引 —— 这是显式的 O(N) 维护成本，
   换取读取时 O(log N) 的稳定定位，且旧索引随旧快照一起保留。
"""
from __future__ import annotations

import mmap
import os
import struct
from dataclasses import dataclass
from typing import List, Optional

MAGIC = b"LIDX"
VERSION = 1
HEADER = struct.Struct("<4sBQI")          # magic, version, generation, count
RID_LEN = 32
SEG_LEN = 64
RECORD = struct.Struct(f"<{RID_LEN}s{SEG_LEN}sIB3xQ")
RECORD_SIZE = RECORD.size                 # 112 字节定长
FLAG_DELETED = 0x01


class IndexError(Exception):
    pass


@dataclass(frozen=True)
class IndexEntry:
    rid: str
    segment: str
    ordinal: int
    deleted: bool
    version: int


class IndexWriter:
    def __init__(self, path: str, generation: int):
        self.path = path
        self.generation = generation
        self._entries: List[IndexEntry] = []

    def add(self, rid: str, segment: str, ordinal: int, version: int, deleted: bool = False) -> None:
        if len(rid) != RID_LEN:
            raise IndexError(f"rid must be {RID_LEN} chars, got {len(rid)}")
        if len(segment) >= SEG_LEN:
            raise IndexError(f"segment name too long for {SEG_LEN}-byte field: {segment}")
        self._entries.append(IndexEntry(rid, segment, ordinal, deleted, version))

    def finish(self) -> str:
        self._entries.sort(key=lambda e: e.rid)
        tmp = self.path + ".tmp"
        with open(tmp, "wb") as fh:
            fh.write(HEADER.pack(MAGIC, VERSION, self.generation, len(self._entries)))
            for e in self._entries:
                fh.write(
                    RECORD.pack(
                        e.rid.encode("ascii"),
                        e.segment.encode("ascii").ljust(SEG_LEN, b"\x00"),
                        e.ordinal,
                        FLAG_DELETED if e.deleted else 0,
                        e.version,
                    )
                )
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, self.path)
        return self.path


class IndexReader:
    """基于 mmap 的只读索引，定位是对定长记录的二分查找 O(log N)。"""

    def __init__(self, path: str):
        self.path = path
        self._fh = open(path, "rb")
        head = self._fh.read(HEADER.size)
        if len(head) < HEADER.size:
            raise IndexError(f"index too short: {path}")
        magic, version, self.generation, self.count = HEADER.unpack(head)
        if magic != MAGIC or version != VERSION:
            raise IndexError(f"bad index header: {path}")
        self._mm = mmap.mmap(self._fh.fileno(), 0, access=mmap.ACCESS_READ)

    def close(self) -> None:
        self._mm.close()
        self._fh.close()

    def __enter__(self) -> "IndexReader":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    def _record(self, i: int) -> IndexEntry:
        rid_b, seg_b, ordinal, flags, version = RECORD.unpack_from(
            self._mm, HEADER.size + i * RECORD_SIZE
        )
        return IndexEntry(
            rid=rid_b.decode("ascii"),
            segment=seg_b.rstrip(b"\x00").decode("ascii"),
            ordinal=ordinal,
            deleted=bool(flags & FLAG_DELETED),
            version=version,
        )

    def lookup(self, rid: str) -> Optional[IndexEntry]:
        key = rid.encode("ascii")
        lo, hi = 0, self.count
        while lo < hi:
            mid = (lo + hi) // 2
            cur = self._mm[HEADER.size + mid * RECORD_SIZE : HEADER.size + mid * RECORD_SIZE + RID_LEN]
            if cur < key:
                lo = mid + 1
            else:
                hi = mid
        if lo < self.count:
            entry = self._record(lo)
            if entry.rid == rid:
                return entry
        return None

    def all(self) -> List[IndexEntry]:
        return [self._record(i) for i in range(self.count)]
