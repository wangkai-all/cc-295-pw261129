"""分区/重排策略。

演示三种物理布局，所有策略只决定行落在哪个段、段内什么顺序，
RID 始终不变 —— 重排后旧链接依旧定位同一条记录。
"""
from __future__ import annotations

import random
from typing import Any, Callable, Dict

# 策略名 -> 函数，供网页下拉框直接选择
REGISTRY: Dict[str, Callable[[Dict[str, Any]], str]] = {}


def register(name: str):
    def deco(fn: Callable[[Dict[str, Any]], str]):
        REGISTRY[name] = fn
        fn.__name__ = name
        return fn
    return deco


@register("by_region")
def by_region(row: Dict[str, Any]) -> str:
    """初始布局：按大区各分一段。"""
    return f"region-{row.get('region', 'unknown')}"


@register("by_region_status")
def by_region_status(row: Dict[str, Any]) -> str:
    """重分区：大区 + 状态，段数量变多、每段变小。"""
    return f"{row.get('region', 'unknown')}-{row.get('status', 'unknown')}"


@register("by_bucket")
def by_bucket(row: Dict[str, Any]) -> str:
    """哈希重分区：故意打散原有的物理邻近关系，验证链接不依赖行号。"""
    rid = row.get("__rid__", "")
    return f"bucket-{int(rid[:8], 16) % 6:02d}"


@register("shuffle")
def make_shuffle(seed: int = 42):
    """随机重排分区器工厂：每次合并用不同种子，物理位置完全洗牌。"""
    rng = random.Random(seed)

    def shuffle_partition(row: Dict[str, Any]) -> str:
        rid = row.get("__rid__", "")
        return f"shuf-{rng.randrange(8):02d}"

    shuffle_partition.__name__ = "shuffle"
    return shuffle_partition


def get_partitioner(name: str, seed: int = 42) -> Callable[[Dict[str, Any]], str]:
    if name == "shuffle":
        return make_shuffle(seed)
    if name not in REGISTRY:
        raise KeyError(f"未知分区策略: {name}，可选: {sorted(REGISTRY)}")
    return REGISTRY[name]
