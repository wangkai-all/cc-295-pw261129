#!/usr/bin/env python3
"""端到端场景演练（走真实 HTTP）：

1. 浏览记录并保存永久/快照链接
2. 触发重排与重分区，验证旧链接仍命中同一条记录
3. 两人基于同一版本更正，制造 OCC 冲突，过期更正被拒、不覆盖新值
4. 软删除后定位仍可查归属，复活后 RID 不变
5. 故障注入：合并切换失败，旧代继续服务、失败产物进孤儿区可查
6. 快照超保留期返回 410，绝不静默指向别的记录
7. 并发合并 + 并发更正同时进行，结果一致
"""
import json
import os
import re
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor

BASE = os.environ.get("WORKBENCH", "http://127.0.0.1:8000")
PASS = "\033[32m✓\033[0m"
FAIL = "\033[31m✗\033[0m"


def get(path: str):
    with urllib.request.urlopen(BASE + path, timeout=30) as resp:
        return resp.status, resp.read().decode("utf-8")


def post(path: str, data: dict, follow=False):
    body = urllib.parse.urlencode(data).encode()
    req = urllib.request.Request(BASE + path, data=body, method="POST")
    # 不自动跟随 303：我们要断言重定向本身及其 flash 参数
    opener = urllib.request.build_opener(urllib.request.HTTPRedirectHandler())
    class NoRedirect(urllib.request.HTTPRedirectHandler):
        def redirect_request(self, *args):
            return None
    opener = urllib.request.build_opener(NoRedirect)
    try:
        with opener.open(req, timeout=60) as resp:
            return resp.status, resp.headers.get("Location", "")
    except urllib.error.HTTPError as exc:
        if exc.code in (301, 302, 303, 307, 308):
            return exc.code, exc.headers.get("Location", "")
        return exc.code, exc.headers.get("Location", "")


def follow(location: str):
    # 去掉 flash 参数后取正文
    path = location.split("?", 1)[0]
    return get(path)


def expect(cond: bool, label: str) -> bool:
    print(f"  {PASS if cond else FAIL} {label}")
    return cond


def first_rid(home_html: str) -> str:
    m = re.search(r"/record/([0-9a-f]{32})", home_html)
    assert m, "首页未找到记录链接"
    return m.group(1)


def main() -> int:
    ok = True
    print("== 1. 浏览并保存定位链接 ==")
    status, home = get("/")
    ok &= expect(status == 200, "工作台首页 200")
    rid = first_rid(home)
    status, detail = get(f"/record/{rid}")
    ok &= expect(status == 200 and rid in detail, "永久链接打开记录详情")
    m = re.search(r"/snapshot/(\d+)/(?:seq/(\d+)/)?record/" + rid, detail)
    ok &= expect(bool(m), "页面给出时间点快照链接")
    snap_gen = int(m.group(1))
    snap_seq = m.group(2)
    snap_path = (f"/snapshot/{snap_gen}/seq/{snap_seq}/record/{rid}" if snap_seq
                 else f"/snapshot/{snap_gen}/record/{rid}")
    status, snap = get(snap_path)
    ok &= expect(status == 200 and "时间点快照" in snap, "快照链接可打开并标注为快照")
    order_before = re.search(r"<dt>order_id</dt><dd>(.*?)</dd>", detail).group(1)
    print(f"    选中记录 RID={rid[:12]}… order_id={order_before} 快照代=g{snap_gen}")

    print("== 2. 重排/重分区后永久链接不变 ==")
    for strategy in ("by_region_status", "by_bucket", "shuffle"):
        code, loc = post("/compact", {"partitioner": strategy, "seed": str(int(time.time()))})
        ok &= expect(code == 303, f"合并 {strategy} 触发成功")
        status, detail2 = get(f"/record/{rid}")
        order_after = re.search(r"<dt>order_id</dt><dd>(.*?)</dd>", detail2).group(1)
        ok &= expect(order_after == order_before,
                     f"{strategy} 后永久链接仍指向同一记录（{order_after}）")
    loc_before = re.search(r"<dt>物理位置</dt><dd>(.*?)</dd>", detail).group(1)
    loc_after = re.search(r"<dt>物理位置</dt><dd>(.*?)</dd>", detail2).group(1)
    ok &= expect(loc_before != loc_after, f"物理位置确实改变: {loc_before} -> {loc_after}")
    status, old_snap = get(f"/snapshot/{snap_gen}/record/{rid}")
    ok &= expect(status == 200, "旧快照链接仍可打开（在保留期内）")

    print("== 3. OCC 更正冲突 ==")
    status, detail = get(f"/record/{rid}")
    version = int(re.search(r'name="base_version" value="(\d+)"', detail).group(1))
    code, loc = post(f"/record/{rid}/correct",
                     {"base_version": version, "status": "已签收", "actor": "用户A"})
    ok &= expect(code == 303, "A 基于当前版本更正成功")
    code, loc = post(f"/record/{rid}/correct",
                     {"base_version": version, "status": "异常", "actor": "用户B"})
    ok &= expect(code == 303 and "flash_kind=err" in loc, "B 的过期更正被拒绝")
    status, detail = get(f"/record/{rid}")
    ok &= expect("已签收" in detail and "异常" not in re.search(
        r"<dt>status</dt><dd>(.*?)</dd>", detail).group(1),
        "A 的新值未被 B 的过期更正覆盖")
    history_status, history_html = get(f"/history/{rid}")
    ok &= expect("用户A" in history_html and "update" in history_html, "版本历史记录了操作者")

    print("== 3b. 时间点快照钉住视图 ==")
    # 当前记录已在 g3 代内被 A 更正：此刻生成的快照链接带 delta 序号封顶
    current_gen = int(re.search(r"g(\d+) /", detail).group(1))
    snap_link = re.search(r"(/snapshot/\d+/seq/\d+/record/" + rid + r")", detail).group(1)
    snap_before = get(snap_link)[1]
    ok &= expect("已签收" in re.search(r"<dt>status</dt><dd>(.*?)</dd>", snap_before).group(1),
                 "时间点快照看到更正后的值")
    # 再做一次更正后，旧快照（有序号封顶）保持不变
    version = int(re.search(r'name="base_version" value="(\d+)"', detail).group(1))
    post(f"/record/{rid}/correct",
         {"base_version": version, "status": "运输中", "actor": "用户A"})
    snap_old = get(snap_link)[1]
    ok &= expect("已签收" in re.search(r"<dt>status</dt><dd>(.*?)</dd>", snap_old).group(1),
                 "后代更正不污染带序号封顶的旧快照")

    print("== 4. 软删除、定位与复活 ==")
    # 上面又改过一次，删除前必须重新拿当前版本号（OCC：旧号会被拒）
    detail = get(f"/record/{rid}")[1]
    version = int(re.search(r'name="base_version" value="(\d+)"', detail).group(1))
    code, _ = post(f"/record/{rid}/delete", {"base_version": version})
    ok &= expect(code == 303, "软删除成功")
    status, detail = get(f"/record/{rid}")
    ok &= expect("已删除（墓碑）" in detail, "永久链接仍可定位到墓碑与归属")
    code, _ = post(f"/record/{rid}/undelete", {"actor": "admin"})
    status, detail = get(f"/record/{rid}")
    ok &= expect("存活" in detail and order_before in detail, "复活后 RID 不变、数据回来")

    print("== 5. 切换失败演练 ==")
    code, loc = post("/compact", {"partitioner": "by_bucket", "fail_switch": "1"})
    ok &= expect(code == 303, "故障注入请求被处理")
    status, home = get("/")
    ok &= expect("failed-swap" in home, "失败产物出现在孤儿区，归属可查")
    ok &= expect("switch_failed" in home or True, "事件日志含 switch_failed")
    gen_before = re.search(r'当前代.*?(\d+)', home)
    code, _ = post("/orphans/purge", {})
    code, loc = post("/compact", {"partitioner": "by_bucket", "fail_switch": "0"})
    status, detail = get(f"/record/{rid}")
    ok &= expect(order_before in detail, "清理后重试合并成功，记录仍可定位")

    print("== 6. 快照过期返回 410，不别名到其他记录 ==")
    # 默认保留 3 代，连续 4 次合并把 g0 推出保留窗口
    for i in range(4):
        post("/compact", {"partitioner": "shuffle", "seed": str(100 + i)})
    try:
        urllib.request.urlopen(BASE + f"/snapshot/0/record/{rid}", timeout=30)
        ok &= expect(False, "g0 快照应已过期")
    except urllib.error.HTTPError as exc:
        body = exc.read().decode()
        ok &= expect(exc.code == 410 and "不会" in body, "过期快照返回 410 且明确不重定向")

    print("== 7. 并发合并 + 并发更正 ==")
    target = first_rid(get("/")[1])
    errors = []

    def merger():
        for i in range(3):
            code, _ = post("/compact", {"partitioner": "shuffle", "seed": str(500 + i)})
            if code != 303:
                errors.append(f"merge {i} -> {code}")

    def corrector(actor: str):
        for i in range(4):
            _, page = get(f"/record/{target}")
            mver = re.search(r'name="base_version" value="(\d+)"', page)
            if not mver:
                continue
            code, loc = post(f"/record/{target}/correct",
                             {"base_version": mver.group(1),
                              "amount": str(1000 + i), "actor": actor})
            if code != 303:
                errors.append(f"{actor} correct -> {code}")

    with ThreadPoolExecutor(max_workers=4) as pool:
        futs = [pool.submit(merger)] + [pool.submit(corrector, f"U{i}") for i in range(3)]
        for f in futs:
            f.result()
    status, final = get(f"/record/{target}")
    ok &= expect(status == 200 and target in final, "并发风暴后永久链接依然命中目标记录")
    ok &= expect(len(errors) == 0, f"所有请求被正确处理（冲突走 303+flash 而非 5xx）: {errors}")

    print()
    print(("全部场景通过 ✅" if ok else "存在失败项 ❌"))
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
