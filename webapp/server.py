"""浏览与更正工作台 Web 服务（仅标准库，ThreadingHTTPServer）。

路由
====
GET  /                     工作台首页：统计 + 记录浏览 + 合并操作面板
GET  /record/<rid>         永久链接：记录详情、物理位置、版本、更正表单
GET  /snapshot/<gen>/record/<rid>  快照链接：固定代视图
GET  /history/<rid>        版本历史与归属审计
POST /compact              触发合并/重排/重分区（可选模拟切换失败）
POST /ingest               追加摄入一批新记录
POST /record/<rid>/correct 更正（带 base_version 的 OCC）
POST /record/<rid>/delete  软删除
POST /record/<rid>/undelete 复活
POST /orphans/purge        清理孤儿区
"""
from __future__ import annotations

import html
import os
import sys
import urllib.parse
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Dict, List, Optional, Tuple

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from colstore import DeltaConflict, NotFound, SnapshotExpired, Table
from colstore.demo_data import SCHEMA, generate_rows
from colstore.partitions import get_partitioner

PARTITION_OPTIONS = [
    ("by_region", "按大区（初始布局）"),
    ("by_region_status", "按大区+状态（重分区）"),
    ("by_bucket", "哈希桶（打散物理邻近）"),
    ("shuffle", "随机洗牌（每次物理位置全变）"),
]


def esc(value: Any) -> str:
    return html.escape("" if value is None else str(value), quote=True)


class App:
    def __init__(self, table: Table):
        self.table = table


# ---------------------------------------------------------------- 页面片段

def page(title: str, body: str, flash: Optional[Tuple[str, str]] = None) -> bytes:
    flash_html = ""
    if flash:
        kind, msg = flash
        flash_html = f'<div class="flash {kind}">{esc(msg)}</div>'
    return f"""<!doctype html>
<html lang="zh-CN"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{esc(title)} - 列存定位工作台</title>
<style>
:root {{ --bg:#0f1420; --panel:#171e2e; --line:#26314a; --fg:#e6ebf5;
        --muted:#8b97ad; --accent:#5b9dff; --ok:#3fbf7f; --warn:#e8a13a;
        --err:#ef6a6a; --mono:"SFMono-Regular",Consolas,"Liberation Mono",monospace; }}
* {{ box-sizing:border-box; }}
body {{ margin:0; background:var(--bg); color:var(--fg);
       font:14px/1.6 -apple-system,"Segoe UI","PingFang SC","Microsoft YaHei",sans-serif; }}
header {{ padding:16px 28px; border-bottom:1px solid var(--line);
         display:flex; gap:16px; align-items:baseline; flex-wrap:wrap; }}
header h1 {{ font-size:18px; margin:0; }}
header a {{ color:var(--accent); text-decoration:none; }}
main {{ max-width:1180px; margin:0 auto; padding:24px 28px 60px; }}
.grid {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(180px,1fr)); gap:12px; }}
.card {{ background:var(--panel); border:1px solid var(--line); border-radius:10px; padding:14px 16px; }}
.card .k {{ color:var(--muted); font-size:12px; }}
.card .v {{ font-size:20px; font-weight:600; margin-top:2px; }}
table {{ width:100%; border-collapse:collapse; background:var(--panel);
        border:1px solid var(--line); border-radius:10px; overflow:hidden; }}
th,td {{ padding:9px 12px; text-align:left; border-bottom:1px solid var(--line); font-size:13px; }}
th {{ color:var(--muted); font-weight:600; background:#121929; }}
tr:last-child td {{ border-bottom:none; }}
td.mono, .mono {{ font-family:var(--mono); font-size:12px; }}
a {{ color:var(--accent); }}
.flash {{ padding:11px 15px; border-radius:8px; margin-bottom:16px; font-size:13px; }}
.flash.ok {{ background:rgba(63,191,127,.12); border:1px solid var(--ok); }}
.flash.err {{ background:rgba(239,106,106,.12); border:1px solid var(--err); }}
.flash.warn {{ background:rgba(232,161,58,.12); border:1px solid var(--warn); }}
form.inline {{ display:inline; }}
input,select,button {{ font:inherit; background:#0d1322; color:var(--fg);
       border:1px solid var(--line); border-radius:7px; padding:7px 10px; }}
button {{ cursor:pointer; background:var(--accent); color:#0b1020; font-weight:600; border:none; }}
button.ghost {{ background:transparent; color:var(--accent); border:1px solid var(--accent); }}
button.danger {{ background:var(--err); color:#1a0b0b; }}
.row {{ display:flex; gap:10px; flex-wrap:wrap; align-items:end; }}
.field {{ display:flex; flex-direction:column; gap:4px; }}
.field label {{ color:var(--muted); font-size:12px; }}
.pill {{ display:inline-block; padding:1px 8px; border-radius:99px; font-size:12px;
        border:1px solid var(--line); color:var(--muted); }}
.pill.del {{ color:var(--err); border-color:var(--err); }}
.pill.live {{ color:var(--ok); border-color:var(--ok); }}
.linkbox {{ font-family:var(--mono); font-size:12px; word-break:break-all;
       background:#0d1322; border:1px solid var(--line); border-radius:7px; padding:8px 10px; }}
section {{ margin-top:26px; }}
h2 {{ font-size:15px; color:var(--muted); text-transform:uppercase; letter-spacing:.06em; }}
dl.kv {{ display:grid; grid-template-columns:160px 1fr; gap:6px 14px; margin:0; }}
dl.kv dt {{ color:var(--muted); }}
dl.kv dd {{ margin:0; font-family:var(--mono); font-size:13px; word-break:break-all; }}
.small {{ font-size:12px; color:var(--muted); }}
</style></head><body>
<header><h1>列存浏览与更正工作台</h1>
<nav><a href="/">工作台</a></nav></header>
<main>{flash_html}{body}</main></body></html>""".encode("utf-8")


def stats_cards(stats: Dict[str, Any]) -> str:
    def card(k: str, v: Any) -> str:
        return f'<div class="card"><div class="k">{esc(k)}</div><div class="v">{esc(v)}</div></div>'
    return '<div class="grid">' + "".join([
        card("当前代 generation", stats["current_generation"]),
        card("存活记录", stats["live_records"]),
        card("索引条目（含墓碑）", stats["index_entries"]),
        card("段数量", len(stats["segments"])),
        card("列存字节", stats["data_bytes"]),
        card("定位索引字节", stats["index_bytes"]),
        card("保留代数", f'{stats["keep_generations"]}（现存 {len(stats["available_generations"])} 代）'),
        card("delta 事件数", stats["delta_events"]),
    ]) + "</div>"


def compact_panel(stats: Dict[str, Any]) -> str:
    options = "".join(
        f'<option value="{v}">{esc(label)}</option>' for v, label in PARTITION_OPTIONS
    )
    return f"""
    <section><h2>合并 / 重排 / 重分区（生成新一代，原子切换）</h2>
    <form method="post" action="/compact" class="card">
      <div class="row">
        <div class="field"><label>物理布局策略</label>
          <select name="partitioner">{options}</select></div>
        <div class="field"><label>随机种子（shuffle 用）</label>
          <input name="seed" value="42" size="8"></div>
        <div class="field"><label>故障注入</label>
          <select name="fail_switch">
            <option value="0">正常切换</option>
            <option value="1">模拟 manifest 切换失败（验证旧代继续服务、产物可查归属）</option>
          </select></div>
        <button type="submit">触发合并</button>
      </div>
      <p class="small">合并期间提交的更正会在写锁上排队，切换后按新版本号校验，过期更正被拒绝。</p>
    </form></section>
    <section><h2>追加摄入</h2>
    <form method="post" action="/ingest" class="card">
      <div class="row">
        <div class="field"><label>新增记录条数</label>
          <input name="count" value="50" size="8"></div>
        <button type="submit">摄入并生成新一代</button>
      </div></form></section>
    <section><h2>孤儿区 / 暂存区（切换失败的遗留，不静默删除）</h2>
    <div class="card">
      <p>孤儿区: {esc(stats["orphans"]) or "<span class=small>空</span>"}<br>
         暂存区: {esc(stats["staged"]) or "<span class=small>空</span>"}</p>
      <form method="post" action="/orphans/purge" class="inline">
        <button class="ghost" type="submit">清理孤儿区</button></form>
    </div></section>"""


def record_table(views, table: Table, limit: int) -> str:
    head = "".join(f"<th>{esc(c)}</th>" for c in SCHEMA)
    rows_html = []
    for v in views[:limit]:
        cells = "".join(f"<td>{esc(v.row.get(c))}</td>" for c in SCHEMA)
        rows_html.append(
            f"<tr><td class=mono><a href='/record/{esc(v.rid)}'>{esc(v.rid[:12])}…</a></td>"
            f"<td><span class='pill live'>v{v.version}</span></td>"
            f"<td class=mono>g{v.location['generation']}/{esc(v.location['segment'])}"
            f"# {v.location['ordinal']}</td>{cells}</tr>"
        )
    return f"""
    <section><h2>记录浏览（前 {min(limit, len(views))} 条，位置来自定位索引而非行号）</h2>
    <table><thead><tr><th>RID（永久标识）</th><th>版本</th><th>当前物理位置</th>{head}
    </tr></thead><tbody>{''.join(rows_html)}</tbody></table></section>"""


# ---------------------------------------------------------------- HTTP 处理器

class Handler(BaseHTTPRequestHandler):
    server_version = "ColStoreWorkbench/1.0"

    @property
    def app(self) -> App:
        return self.server.app  # type: ignore[attr-defined]

    def log_message(self, fmt: str, *args: Any) -> None:
        sys.stderr.write("[http] " + fmt % args + "\n")

    def _send(self, body: bytes, status: int = 200,
              extra_headers: Optional[Dict[str, str]] = None) -> None:
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        for k, val in (extra_headers or {}).items():
            self.send_header(k, val)
        self.end_headers()
        self.wfile.write(body)

    def _redirect(self, location: str, flash: Optional[Tuple[str, str]] = None) -> None:
        if flash:
            q = urllib.parse.urlencode({"flash_kind": flash[0], "flash_msg": flash[1]})
            sep = "&" if "?" in location else "?"
            location = location + sep + q
        self._send(b"", HTTPStatus.SEE_OTHER, {"Location": location})

    def _flash(self, params: Dict[str, List[str]]) -> Optional[Tuple[str, str]]:
        if "flash_msg" in params:
            return (params.get("flash_kind", ["ok"])[0], params["flash_msg"][0])
        return None

    def _form(self) -> Dict[str, str]:
        length = int(self.headers.get("Content-Length", 0))
        raw = self.rfile.read(length).decode("utf-8") if length else ""
        return {k: v[0] for k, v in urllib.parse.parse_qs(raw).items()}

    # ---------- GET ----------

    def do_GET(self) -> None:  # noqa: N802
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        params = urllib.parse.parse_qs(parsed.query)
        try:
            if path == "/":
                self._send(page("工作台", self._home(params), self._flash(params)))
            elif path.startswith("/record/"):
                rid = path.split("/")[2]
                self._send(page("记录详情", self._detail(rid, None), self._flash(params)))
            elif path.startswith("/snapshot/"):
                parts = path.strip("/").split("/")
                # 支持 /snapshot/<gen>/record/<rid>（代切换时刻）与
                # /snapshot/<gen>/seq/<n>/record/<rid>（精确时间点）两种形式
                gen = int(parts[1])
                if parts[2] == "seq":
                    max_seq = int(parts[3])
                    rid = parts[5]
                else:
                    max_seq = None
                    rid = parts[3]
                self._send(page(f"快照 g{gen}", self._detail(rid, gen, max_seq),
                                self._flash(params)))
            elif path.startswith("/history/"):
                rid = path.split("/")[2]
                self._send(page("版本历史", self._history(rid), self._flash(params)))
            else:
                self._send(page("404", "<p>页面不存在</p>"), HTTPStatus.NOT_FOUND)
        except NotFound as exc:
            self._send(page("未找到", f"<div class='flash err'>{esc(str(exc))}</div>"),
                       HTTPStatus.NOT_FOUND)
        except SnapshotExpired as exc:
            self._send(page("快照过期", self._expired(exc)), HTTPStatus.GONE)

    def _home(self, params: Dict[str, List[str]]) -> str:
        table = self.app.table
        stats = table.stats()
        views = table.scan(limit=25)
        return (stats_cards(stats) + compact_panel(stats) + record_table(views, table, 25)
                + self._segment_list(stats))

    def _segment_list(self, stats: Dict[str, Any]) -> str:
        items = "".join(
            f"<tr><td class=mono>{esc(s['name'])}</td><td>{esc(s['partition'])}</td>"
            f"<td>{s['rows']}</td><td>{s['bytes']}</td></tr>"
            for s in stats["segments"]
        )
        return f"""<section><h2>当前代段清单（manifest）</h2>
        <table><thead><tr><th>段 ID</th><th>分区键</th><th>行数</th><th>字节</th></tr></thead>
        <tbody>{items}</tbody></table></section>"""

    def _detail(self, rid: str, snapshot_gen: Optional[int],
                snapshot_seq: Optional[int] = None) -> str:
        table = self.app.table
        if snapshot_gen is not None:
            view = table.locate_snapshot(rid, snapshot_gen, max_seq=snapshot_seq)
            banner = (f"<div class='flash warn'>您正在浏览第 <b>{snapshot_gen}</b> 代"
                      f"{'、序号 ≤ ' + str(snapshot_seq) if snapshot_seq is not None else '切换时刻'}"
                      f"的时间点快照，数据已冻结；<a href='/record/{esc(rid)}'>查看当前态</a></div>")
        else:
            view = table.locate(rid)
            banner = ""
        perm_link = f"/record/{rid}"
        snap_gen = snapshot_gen if snapshot_gen is not None else table.stats()["current_generation"]
        # 当前态详情页生成的快照链接钉在“此刻”：记录当前版本对应的 delta 序号
        if snapshot_seq is not None:
            snap_link = f"/snapshot/{snap_gen}/seq/{snapshot_seq}/record/{rid}"
        else:
            ceiling = max(
                (e["seq"] for e in table.history(rid)
                 if e["base_generation"] <= snap_gen),
                default=-1,
            )
            snap_link = f"/snapshot/{snap_gen}/seq/{ceiling}/record/{rid}"
        status = ("<span class='pill del'>已删除（墓碑）</span>" if view.deleted
                  else "<span class='pill live'>存活</span>")
        kv = "".join(
            f"<dt>{esc(c)}</dt><dd>{esc(view.row.get(c) if view.row else None)}</dd>"
            for c in SCHEMA
        )
        loc = view.location or {}
        loc_html = (
            f"<dt>物理位置</dt><dd>g{loc.get('generation')} / {esc(loc.get('segment'))}"
            f" / ordinal {loc.get('ordinal')}</dd>"
        ) if loc else "<dt>物理位置</dt><dd>无（墓碑或尚未物化）</dd>"
        form = self._correct_form(rid, view, snapshot_gen)
        delete_btn = ""
        if not view.deleted and snapshot_gen is None:
            delete_btn = f"""
            <form method="post" action="/record/{esc(rid)}/delete" class="inline">
              <input type="hidden" name="base_version" value="{view.version}">
              <button class="danger" type="submit">软删除该记录</button></form>"""
        undelete_btn = (
            f'<form method="post" action="/record/{esc(rid)}/undelete" class="inline">'
            f'<button type="submit">复活（RID 与历史不变）</button></form>'
            if view.deleted and snapshot_gen is None else ""
        )
        return f"""{banner}
        <section class="card">
          <h2>记录 {esc(rid)}</h2>
          <p>{status} <span class="pill">当前版本 v{view.version}</span>
             <a class="pill" href="/history/{esc(rid)}">查看版本历史与归属</a></p>
          <dl class="kv">
            <dt>RID（永久逻辑标识）</dt><dd>{esc(rid)}</dd>
            {loc_html}
            <dt>所在代</dt><dd>g{view.generation}</dd>
            {kv}
          </dl>
        </section>
        <section class="card">
          <h2>可保存的定位链接</h2>
          <p class="small">永久链接：与物理行号无关，合并/重排/重分区后永远指向本记录</p>
          <div class="linkbox">{esc(perm_link)}</div>
          <p class="small">时间点快照：固定第 {snap_gen} 代物理布局 + 此刻的 delta 序号；
             之后的更正不影响它，超保留期会显式报 GONE，不会误指别的记录</p>
          <div class="linkbox">{esc(snap_link)}</div>
        </section>
        <section class="card"><h2>发起更正（乐观并发，基于 v{view.version}）</h2>
          {form}{delete_btn}{undelete_btn}</section>"""

    def _correct_form(self, rid: str, view, snapshot_gen: Optional[int]) -> str:
        if snapshot_gen is not None:
            return "<p class='small'>快照视图为只读，不能更正。</p>"
        if view.deleted:
            return "<p class='small'>记录已删除，请先复活再更正。</p>"
        fields = "".join(
            f'<div class="field"><label>{esc(c)}（{esc(SCHEMA[c])}）</label>'
            f'<input name="{esc(c)}" value="{esc(view.row.get(c))}"></div>'
            for c in SCHEMA
        )
        return f"""
        <form method="post" action="/record/{esc(rid)}/correct">
          <input type="hidden" name="base_version" value="{view.version}">
          <div class="row">{fields}
            <div class="field"><label>操作者</label><input name="actor" value="analyst"></div>
            <button type="submit">提交更正</button></div>
          <p class="small">提交时服务端校验 base_version 是否等于当前版本；他人先改过时返回冲突，本表单不会覆盖。</p>
        </form>"""

    def _history(self, rid: str) -> str:
        table = self.app.table
        events = table.history(rid)
        rows = []
        for e in events:
            row = e.get("row") or {}
            summary = f"order_id={row.get('order_id', '')}" if row else "-"
            rows.append(
                f"<tr><td>{e['seq']}</td><td>{esc(e['ts'])}</td><td>{esc(e['type'])}</td>"
                f"<td>{esc(e['actor'])}</td><td>v{esc(e['base_version'])} → v{e['new_version']}</td>"
                f"<td>g{e['base_generation']}</td>"
                f"<td class=mono>{esc(summary)}</td></tr>"
            )
        sys_events = [
            ev for ev in table.system_events(limit=200)
            if ev.get("detail", {}).get("rid") == rid
        ]
        sys_rows = "".join(
            f"<tr><td>{esc(ev['ts'])}</td><td>{esc(ev['type'])}</td>"
            f"<td class=mono>{esc(ev['detail'])}</td></tr>" for ev in sys_events
        ) or "<tr><td colspan=3 class=small>无表级事件</td></tr>"
        return f"""
        <section class="card"><h2>RID {esc(rid)} 的记录级版本链</h2>
        <table><thead><tr><th>seq</th><th>时间</th><th>事件</th><th>操作者</th>
        <th>版本变迁</th><th>提交时代</th><th>摘要</th></tr></thead>
        <tbody>{''.join(rows) or '<tr><td colspan=7 class=small>无</td></tr>'}</tbody></table></section>
        <section class="card"><h2>表级审计事件（含合并/切换/失败）</h2>
        <table><thead><tr><th>时间</th><th>类型</th><th>详情</th></tr></thead>
        <tbody>{sys_rows}</tbody></table>
        <p><a href="/record/{esc(rid)}">← 返回记录详情</a></p></section>"""

    def _expired(self, exc: SnapshotExpired) -> str:
        return f"""
        <section class="card">
          <h2>快照链接已过期（HTTP 410 GONE）</h2>
          <p>第 <b>{exc.generation}</b> 代快照已超出历史保留范围，现存代：{esc(exc.available)}。</p>
          <p>系统<b>不会</b>把这个旧链接静默指向当前的另一条记录 —— 定位以永久 RID 为准，
             请通过记录的永久链接访问当前态。</p>
          <p><a href="/">← 返回工作台</a></p></section>"""

    # ---------- POST ----------

    def do_POST(self) -> None:  # noqa: N802
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        form = self._form()
        table = self.app.table
        try:
            if path == "/compact":
                self._handle_compact(form)
            elif path == "/ingest":
                self._handle_ingest(form)
            elif path == "/orphans/purge":
                table.store.purge_orphans()
                self._redirect("/", ("ok", "孤儿区已清理"))
            elif path.endswith("/correct"):
                self._handle_correct(path, form)
            elif path.endswith("/delete"):
                self._handle_delete(path, form)
            elif path.endswith("/undelete"):
                rid = path.split("/")[2]
                table.undelete(rid, actor=form.get("actor", "analyst"))
                self._redirect(f"/record/{rid}", ("ok", "记录已复活，RID 与版本历史保持不变"))
            else:
                self._send(page("404", "<p>接口不存在</p>"), HTTPStatus.NOT_FOUND)
        except DeltaConflict as exc:
            rid = path.split("/")[2] if "/record/" in path else ""
            self._redirect(f"/record/{rid}", ("err", str(exc)))
        except NotFound as exc:
            self._send(page("未找到", f"<div class='flash err'>{esc(str(exc))}</div>"),
                       HTTPStatus.NOT_FOUND)
        except (ValueError, KeyError) as exc:
            self._redirect("/", ("err", f"参数错误: {esc(str(exc))}"))

    def _handle_compact(self, form: Dict[str, str]) -> None:
        table = self.app.table
        partitioner = get_partitioner(form["partitioner"], int(form.get("seed", "42")))
        old_hook = table.pre_swap_hook
        if form.get("fail_switch") == "1":
            table.pre_swap_hook = lambda gen: (_ for _ in ()).throw(
                RuntimeError("演练：manifest 切换失败"))
        try:
            try:
                gen = table.compact(partitioner, actor=form.get("actor", "web-user"),
                                    mode=form["partitioner"])
            except RuntimeError as exc:
                # 引擎已把暂存产物隔离进孤儿区并记录 switch_failed，旧代仍在服务
                self._redirect("/", ("err", f"合并切换失败（演练）: {esc(str(exc))}；"
                                            "旧代继续服务，失败产物已进孤儿区可查归属"))
                return
        finally:
            table.pre_swap_hook = old_hook
        self._redirect("/", ("ok", f"合并完成，已切换到第 {gen} 代（RID 全部保持不变）"))

    def _handle_ingest(self, form: Dict[str, str]) -> None:
        table = self.app.table
        count = int(form.get("count", "50"))
        rows = generate_rows(count, seed=table.deltas.next_seq + 7)
        gen = table.ingest(rows, get_partitioner("by_region"),
                           actor=form.get("actor", "web-user"))
        self._redirect("/", ("ok", f"已摄入 {count} 条记录并生成第 {gen} 代"))

    def _handle_correct(self, path: str, form: Dict[str, str]) -> None:
        table = self.app.table
        rid = path.split("/")[2]
        base_version = int(form["base_version"])
        current = table.locate(rid).row or {}
        from colstore.encoding import cast_value
        changes = {}
        for col, dtype in SCHEMA.items():
            if col in form:
                new_val = cast_value(form[col], dtype)
                if new_val != current.get(col):
                    changes[col] = new_val
        if not changes:
            self._redirect(f"/record/{rid}", ("warn", "没有字段发生变化"))
            return
        table.correct(rid, changes, base_version, actor=form.get("actor", "analyst"))
        self._redirect(f"/record/{rid}",
                       ("ok", f"更正成功，改动字段: {', '.join(changes)}，版本已推进"))

    def _handle_delete(self, path: str, form: Dict[str, str]) -> None:
        table = self.app.table
        rid = path.split("/")[2]
        table.delete(rid, int(form["base_version"]), actor=form.get("actor", "analyst"))
        self._redirect(f"/record/{rid}", ("ok", "已软删除：扫描不可见，但定位与历史仍可查归属"))


def serve(root: str, host: str = "127.0.0.1", port: int = 8000) -> None:
    table = Table(root, keep_generations=3)
    httpd = ThreadingHTTPServer((host, port), Handler)
    httpd.app = App(table)  # type: ignore[attr-defined]
    print(f"工作台已启动: http://{host}:{port}/  数据目录: {root}")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\n关闭中…")
    finally:
        table.close()
        httpd.server_close()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("root", nargs="?", default=os.path.join(os.getcwd(), "data_store"))
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    args = parser.parse_args()
    serve(args.root, args.host, args.port)
