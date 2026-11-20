"""真实列存段（segment）：每列一个独立 chunk，段尾保存列偏移目录。

段文件布局
==========
  magic 'CSEG' + version(u8) + row_count(u32)
  column chunk 0..k（每块 = encoding.Values 帧，可独立解压）
  footer:
      col_count(u32)
      col_name_len(u16) + name(utf8) + offset(u64) + length(u664)  每列一组
  footer_len(u32) + magic 'GSEC'

读取时先从尾部找 footer，按需只解压被投影的列 —— 这就是列存相对于行存的
核心收益：查询不涉及的列不会被读入内存。
"""
from __future__ import annotations

import os
import struct
from typing import Any, Dict, List, Optional, Sequence

from . import encoding

MAGIC = b"CSEG"
FOOTER_MAGIC = b"GSEC"
VERSION = 1
HEADER = struct.Struct("<4sBI")          # magic, version, row_count
FOOTER_ENTRY = struct.Struct("<H")        # name length
FOOTER_OFF = struct.Struct("<QQ")         # offset, length
FOOTER_TAIL = struct.Struct("<I4s")       # footer_len, footer magic


class SegmentError(Exception):
    pass


class SegmentWriter:
    def __init__(self, path: str, schema: "Dict[str, str]"):
        self.path = path
        self.schema = dict(schema)
        self._columns: Dict[str, List[Any]] = {name: [] for name in self.schema}
        self._row_count = 0

    def add_row(self, row: Dict[str, Any]) -> None:
        for name, dtype in self.schema.items():
            value = row.get(name)
            if value is not None and not isinstance(value, (int, float, bool, str)):
                raise SegmentError(f"unsupported value type for {name}: {type(value)}")
            self._columns[name].append(value)
        self._row_count += 1

    def add_rows(self, rows: Sequence[Dict[str, Any]]) -> None:
        for row in rows:
            self.add_row(row)

    def finish(self) -> str:
        tmp = self.path + ".tmp"
        with open(tmp, "wb") as fh:
            fh.write(HEADER.pack(MAGIC, VERSION, self._row_count))
            entries = []
            for name, dtype in self.schema.items():
                offset = fh.tell()
                blob = encoding.encode_values(self._columns[name], dtype)
                fh.write(blob)
                entries.append((name, offset, len(blob)))
            footer_pos = fh.tell()
            fh.write(struct.pack("<I", len(entries)))
            for name, offset, length in entries:
                encoded = name.encode("utf-8")
                fh.write(FOOTER_ENTRY.pack(len(encoded)) + encoded + FOOTER_OFF.pack(offset, length))
            footer = fh.tell() - footer_pos
            fh.write(FOOTER_TAIL.pack(footer, FOOTER_MAGIC))
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, self.path)
        return self.path

    @property
    def row_count(self) -> int:
        return self._row_count


class SegmentReader:
    """内存映射风格的段读取器：打开后常驻 footer，列按需解压并缓存。"""

    def __init__(self, path: str):
        self.path = path
        with open(path, "rb") as fh:
            head = fh.read(HEADER.size)
            if len(head) < HEADER.size:
                raise SegmentError(f"segment too short: {path}")
            magic, version, self.row_count = HEADER.unpack(head)
            if magic != MAGIC or version != VERSION:
                raise SegmentError(f"bad segment header in {path}")
            tail_size = FOOTER_TAIL.size
            fh.seek(-tail_size, os.SEEK_END)
            footer_len, footer_magic = FOOTER_TAIL.unpack(fh.read(tail_size))
            if footer_magic != FOOTER_MAGIC:
                raise SegmentError(f"bad segment footer in {path}")
            fh.seek(-(tail_size + footer_len), os.SEEK_END)
            footer_blob = fh.read(footer_len)
            (col_count,) = struct.unpack_from("<I", footer_blob, 0)
            pos = 4
            self.columns: Dict[str, tuple] = {}
            for _ in range(col_count):
                (name_len,) = FOOTER_ENTRY.unpack_from(footer_blob, pos)
                pos += FOOTER_ENTRY.size
                name = footer_blob[pos : pos + name_len].decode("utf-8")
                pos += name_len
                offset, length = FOOTER_OFF.unpack_from(footer_blob, pos)
                pos += FOOTER_OFF.size
                self.columns[name] = (offset, length)
        self._cache: Dict[str, List[Any]] = {}

    def read_column(self, name: str) -> List[Optional[Any]]:
        if name in self._cache:
            return self._cache[name]
        if name not in self.columns:
            raise SegmentError(f"column {name!r} not found in {self.path}")
        offset, length = self.columns[name]
        with open(self.path, "rb") as fh:
            fh.seek(offset)
            blob = fh.read(length)
        dtype = self._infer_dtype(name, blob)
        values = encoding.decode_values(blob, dtype)
        self._cache[name] = values
        return values

    def _infer_dtype(self, name: str, blob: bytes) -> str:
        # dtype 不写进 footer（保持目录精简），由表引擎在 read_rows 时显式传入；
        # 这里通过外部注入的 schema 解析。
        schema = getattr(self, "schema", None)
        if schema and name in schema:
            return schema[name]
        raise SegmentError(f"no schema attached for dtype inference of {name!r}")

    def read_rows(
        self,
        schema: "Dict[str, str]",
        columns: Optional[Sequence[str]] = None,
    ) -> List[Dict[str, Any]]:
        """物化整段为行字典；columns 为 None 时投影全部列。"""
        self.schema = schema
        wanted = list(columns) if columns else list(schema)
        materialized = {name: self.read_column(name) for name in wanted}
        rows: List[Dict[str, Any]] = []
        for i in range(self.row_count):
            rows.append({name: materialized[name][i] for name in wanted})
        return rows

    def read_row(self, schema: "Dict[str, str]", ordinal: int) -> Dict[str, Any]:
        if not 0 <= ordinal < self.row_count:
            raise SegmentError(f"ordinal {ordinal} out of range [0, {self.row_count})")
        self.schema = schema
        return {name: self.read_column(name)[ordinal] for name in schema}
