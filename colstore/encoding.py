"""列存底层编解码：null 位图 + 按列二进制编码 + 可选 zlib 压缩。

所有编码均为小端序（LE），不依赖任何第三方库。
支持的逻辑类型：int64 / float64 / bool / str。
"""
from __future__ import annotations

import struct
import zlib
from typing import Any, List, Optional, Tuple

# 逻辑类型 -> 数值载体的 struct 格式
_TYPE_STRUCT = {
    "int64": "<q",
    "float64": "<d",
    "bool": "<B",
}

# chunk 帧头: flags(uint8) + raw_len(uint32) + payload_len(uint32)
FRAME_HEADER = struct.Struct("<BII")
FLAG_ZLIB = 0x01
# 原始块小于该值时不压缩（压缩小数据通常反而变大）
COMPRESS_THRESHOLD = 256


class CodecError(Exception):
    pass


def encode_null_bitmap(values: List[Optional[Any]]) -> bytes:
    """把每个值的空值情况压成位图，第 i 位为 1 表示该值为 None。"""
    n = len(values)
    bitmap = bytearray((n + 7) // 8)
    for i, v in enumerate(values):
        if v is None:
            bitmap[i // 8] |= 1 << (i % 8)
    return bytes(bitmap)


def decode_null_bitmap(bitmap: bytes, n: int) -> List[bool]:
    return [bool(bitmap[i // 8] & (1 << (i % 8))) for i in range(n)]


def encode_values(values: List[Optional[Any]], dtype: str) -> bytes:
    """按列编码一整列的值（含 null 位图与数据载体）。"""
    n = len(values)
    nulls = encode_null_bitmap(values)
    if dtype == "str":
        payload = b"".join(
            b"\x00\x00\x00\x00" if v is None else struct.pack("<I", len(v.encode("utf-8"))) + v.encode("utf-8")
            for v in values
        )
    elif dtype in _TYPE_STRUCT:
        fmt = _TYPE_STRUCT[dtype]
        default = False if dtype == "bool" else 0
        payload = b"".join(struct.pack(fmt, default if v is None else v) for v in values)
    else:
        raise CodecError(f"unknown dtype: {dtype}")
    raw = struct.pack("<I", n) + nulls + payload
    return _frame(raw)


def decode_values(blob: bytes, dtype: str) -> List[Optional[Any]]:
    flags, raw_len, payload_len = FRAME_HEADER.unpack_from(blob, 0)
    raw = blob[FRAME_HEADER.size : FRAME_HEADER.size + payload_len]
    if flags & FLAG_ZLIB:
        raw = zlib.decompress(raw)
    if len(raw) < raw_len:
        raise CodecError("corrupt chunk: declared raw length exceeds buffer")
    (n,) = struct.unpack_from("<I", raw, 0)
    bitmap_len = (n + 7) // 8
    nulls = decode_null_bitmap(raw[4 : 4 + bitmap_len], n)
    pos = 4 + bitmap_len
    out: List[Optional[Any]] = []
    if dtype == "str":
        for i in range(n):
            if nulls[i]:
                out.append(None)
                pos += 4  # 跳过 null 占位的 4 字节长度槽
                continue
            (length,) = struct.unpack_from("<I", raw, pos)
            pos += 4
            out.append(raw[pos : pos + length].decode("utf-8"))
            pos += length
    elif dtype in _TYPE_STRUCT:
        fmt = _TYPE_STRUCT[dtype]
        width = struct.calcsize(fmt)
        for i in range(n):
            if nulls[i]:
                out.append(None)
            else:
                out.append(struct.unpack_from(fmt, raw, pos)[0])
                if dtype == "bool":
                    out[-1] = bool(out[-1])
            pos += width
    else:
        raise CodecError(f"unknown dtype: {dtype}")
    return out


def _frame(raw: bytes) -> bytes:
    if len(raw) >= COMPRESS_THRESHOLD:
        compressed = zlib.compress(raw, level=6)
        if len(compressed) < len(raw):
            return FRAME_HEADER.pack(FLAG_ZLIB, len(raw), len(compressed)) + compressed
    return FRAME_HEADER.pack(0, len(raw), len(raw)) + raw


def cast_value(value: str, dtype: str) -> Any:
    """把 HTTP 表单里的字符串转成列类型（空串视为 None）。"""
    if value == "":
        return None
    if dtype == "int64":
        return int(value)
    if dtype == "float64":
        return float(value)
    if dtype == "bool":
        return value.strip().lower() in ("1", "true", "yes", "on", "是", "真")
    if dtype == "str":
        return value
    raise CodecError(f"unknown dtype: {dtype}")
