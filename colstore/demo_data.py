"""演示数据集：物流运单。固定随机种子，保证每次重建内容一致。"""
from __future__ import annotations

import random
from typing import Any, Dict, List

SCHEMA: Dict[str, str] = {
    "order_id": "str",
    "customer": "str",
    "region": "str",
    "city": "str",
    "product": "str",
    "status": "str",
    "amount": "int64",
    "weight_kg": "float64",
    "insured": "bool",
}

REGIONS = {
    "华东": ["上海", "杭州", "南京", "苏州"],
    "华北": ["北京", "天津", "石家庄"],
    "华南": ["广州", "深圳", "厦门"],
    "西南": ["成都", "重庆", "昆明"],
}
STATUSES = ["待揽收", "运输中", "派送中", "已签收", "异常"]
PRODUCTS = ["电子元件", "医疗器械", "生鲜冷链", "工业耗材", "服装鞋帽", "精密仪器"]
CUSTOMERS = ["恒远贸易", "启明科技", "蓝湾物流", "北辰制造", "南粤供应链", "云岭农业"]


def generate_rows(count: int = 2000, seed: int = 20261120) -> List[Dict[str, Any]]:
    rng = random.Random(seed)
    rows = []
    for i in range(1, count + 1):
        region = rng.choice(list(REGIONS))
        rows.append({
            "order_id": f"SF{i:06d}",
            "customer": rng.choice(CUSTOMERS),
            "region": region,
            "city": rng.choice(REGIONS[region]),
            "product": rng.choice(PRODUCTS),
            "status": rng.choice(STATUSES),
            "amount": rng.randint(50, 20000),
            "weight_kg": round(rng.uniform(0.2, 480.0), 2),
            "insured": rng.random() < 0.35,
        })
    return rows
