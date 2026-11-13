"""真实场景数据:两个真 magpie,带 compact/分支/多会话,过一条限速链路。

这是 ``tools/native.py`` 的完整版,回答三个前面没回答的问题:

1. **不只是纯追加**。轨迹里含 compact、回退分支、中途新增工具、并行会话
   (``tools/traffic.py`` 生成),所以数据不会被「理想情况」美化:能看出一类事件
   让前缀失效时要付多少。
2. **不只是字节**。中间那条 TCP 链路按真实上行带宽限速(``tools/link.py``),
   于是「少传 90% 字节」直接变成「上传快多少」;每个请求的上传耗时是这条连接上
   字节真正传完的时间,不是端到端总时间。
3. **两条路同一个观测点**。plain(整包 JSON)和 tail(``tail=on``)走同一个中继、
   同一批请求、同一个上游;推理服务收到的 messages 逐轮对账。

拓扑::

    agent/脚本  ──▶  magpie(本地)  ──▶  限速中继  ──▶  magpie(集群)  ──▶ 推理服务
                        ├── provider plain:url=…            (MAGPIE_TAIL=1)
                        └── provider tail :url=… tail=on

用法::

    .venv/bin/python -m tools.realworld --magpie /tmp/magpie-tail \\
        --rounds 24 --mbps 25 --compact-every 8 --branch-every 6 --sessions 2

``--mbps 0`` 就是纯回环(只测 CPU 开销),``--mbps 25`` 是一条普通家宽上行。
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time
from typing import Dict, List, Optional, Tuple

import httpx2 as httpx
import uvicorn

from .link import ThrottledRelay, upload_seconds
from .traffic import Turn, build_trace, payload_bytes

from .magpie_pair import Server, _free_port


# ---------------------------------------------------------------------------
# magpie 进程(与 native.py 同一套,复用不了就照抄——它只有几十行)
# ---------------------------------------------------------------------------

class Magpie:
    def __init__(self, binary: str, root: str, addr: str, tail: bool = False):
        self.binary, self.root, self.addr = binary, root, addr
        self.tail = "1" if tail else ""
        self.proc: Optional[subprocess.Popen] = None
        for d in ("config", "cache"):
            os.makedirs(os.path.join(root, d), exist_ok=True)

    def env(self) -> Dict[str, str]:
        e = dict(os.environ)
        e["XDG_CONFIG_HOME"] = os.path.join(self.root, "config")
        e["XDG_CACHE_HOME"] = os.path.join(self.root, "cache")
        e["MAGPIE_ADDR"] = self.addr
        e["MAGPIE_TAIL"] = self.tail
        return e

    def provider(self, name: str, url: str, model: str, tail: bool) -> None:
        kv = [f"url={url}", f"key=sk-{name}-test", f"models={model}"]
        if tail:
            kv.append("tail=on")
        out = subprocess.run([self.binary, "provider", "add", name, *kv],
                             env=self.env(), capture_output=True, text=True,
                             timeout=180)
        if out.returncode != 0:
            raise RuntimeError(f"provider add {name}: {out.stderr or out.stdout}")

    def start(self, log: Optional[str] = None) -> None:
        """起 serve。传 log 就把 stderr 落到那个文件(调试用)。"""
        err = open(log, "wb") if log else subprocess.DEVNULL
        self.proc = subprocess.Popen([self.binary, "serve"], env=self.env(),
                                     stdout=subprocess.DEVNULL, stderr=err)

    def wait(self, timeout: float = 40) -> None:
        url = f"http://{self.addr}/v1/models"
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                if httpx.get(url, timeout=2).status_code == 200:
                    return
            except Exception:                   # noqa: BLE001
                pass
            time.sleep(0.2)
        raise RuntimeError(f"magpie {self.addr} 没起来")

    def models(self) -> List[str]:
        r = httpx.get(f"http://{self.addr}/v1/models", timeout=15)
        r.raise_for_status()
        return [m["id"] for m in r.json()["data"]]

    def stop(self) -> None:
        if self.proc is not None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.proc.kill()


# ---------------------------------------------------------------------------
# 发一轮
# ---------------------------------------------------------------------------

def send(gateway_url: str, turn: Turn, model: str, stream: bool) -> float:
    payload = turn.payload(model, stream)
    body = payload_bytes(payload)
    t0 = time.perf_counter()
    # 一个请求一条连接(Connection: close):中继按连接记一次请求的上传耗时,
    # keep-alive 会把几个请求混进同一条记录里
    with httpx.Client(timeout=600, headers={"connection": "close"}) as client:
        r = client.post(gateway_url, content=body,
                        headers={"content-type": "application/json",
                                 "authorization": "Bearer magpie"})
        r.raise_for_status()
    return (time.perf_counter() - t0) * 1000


def run_pass(gateway_url: str, model: str, trace, relay: "ThrottledRelay",
             stream: bool) -> List[Dict]:
    """跑一遍完整轨迹,逐轮记录:字节、上传耗时、端到端。

    magpie 到中继是长连接,所以每个请求的上传耗时取自中继切出来的**时间窗口**
    (见 tools/link.py),而不是「一条连接一次」。
    """
    rows: List[Dict] = []
    relay.reset()
    for turn in trace.turns:
        # 请求是严格串行发的(send 等响应回来才返回),所以「这次 send 期间
        # 到中继的字节」就是这一个请求的上传。send 返回时服务端已经收全了
        # 请求体,最多再等一两毫秒让中继落最后一段。
        ms = send(gateway_url, turn, model, stream)
        windows = []
        for _ in range(40):
            windows = relay.take_windows(settle=0.0)
            if windows:
                break
            time.sleep(0.002)
        json_bytes = len(payload_bytes(turn.payload(model, stream)))
        up = sum(w.up_bytes for w in windows)
        upload_ms = max((w.seconds for w in windows), default=0.0) * 1000
        rows.append({"kind": turn.kind, "session": turn.session,
                     "json_bytes": json_bytes, "wire_bytes": up,
                     "upload_ms": round(upload_ms, 1),
                     "total_ms": round(ms, 1)})
    return rows


# ---------------------------------------------------------------------------
# 报表
# ---------------------------------------------------------------------------

def _kb(n: float) -> str:
    return f"{n / 1024:.1f}"


def _mb(n: float) -> str:
    return f"{n / 1024 / 1024:.2f}"


def by_kind(rows: List[Dict]) -> Dict[str, Dict[str, float]]:
    out: Dict[str, Dict[str, float]] = {}
    for r in rows:
        d = out.setdefault(r["kind"], {"n": 0, "json": 0, "wire": 0,
                                       "upload_ms": 0.0})
        d["n"] += 1
        d["json"] += r["json_bytes"]
        d["wire"] += r["wire_bytes"]
        d["upload_ms"] += r["upload_ms"]
    return out


def report(plain: List[Dict], tail: List[Dict], mbps: float,
           trace_kinds: Dict[str, int]) -> dict:
    n = len(plain)
    print()
    print("=" * 100)
    print(f"{'轮':>3} {'事件':>10} {'JSON':>9} {'plain上行':>10} {'tail上行':>10} "
          f"{'plain上传':>10} {'tail上传':>10} {'省':>7}")
    print(f"{'':>3} {'':>10} {'(KB)':>9} {'(KB)':>10} {'(KB)':>10} "
          f"{'(ms)':>10} {'(ms)':>10} {'(倍)':>7}")
    print("-" * 100)
    for i, (a, b) in enumerate(zip(plain, tail), 1):
        ratio = a["wire_bytes"] / b["wire_bytes"] if b["wire_bytes"] else 0
        print(f"{i:>3} "
              f"{a['kind']:>10} {_kb(a['json_bytes']):>9} "
              f"{_kb(a['wire_bytes']):>10} {_kb(b['wire_bytes']):>10} "
              f"{a['upload_ms']:>10.1f} {b['upload_ms']:>10.1f} {ratio:>6.1f}x")
    print("-" * 100)

    pj = sum(r["json_bytes"] for r in plain)
    pw = sum(r["wire_bytes"] for r in plain)
    tw = sum(r["wire_bytes"] for r in tail)
    pu = sum(r["upload_ms"] for r in plain)
    tu = sum(r["upload_ms"] for r in tail)
    print(f"合计 {n} 轮:要发的 JSON {_mb(pj)} MB → plain 上线 {_mb(pw)} MB,"
          f"tail 上线 {_mb(tw)} MB(**{pw / tw:.1f}x**)")
    print(f"上传耗时(实测,{mbps or '无'} Mbps 链路):plain {pu / 1000:.2f} s,"
          f"tail {tu / 1000:.2f} s,省 {(pu - tu) / 1000:.2f} s")
    if mbps:
        model_p = upload_seconds(pw, mbps)
        model_t = upload_seconds(tw, mbps)
        print(f"  纯算术对照:按 {mbps} Mbps 算应分别 {model_p:.2f} s / "
              f"{model_t:.2f} s(实测含协议开销与排队,略高)")

    # —— 按事件类别拆开:哪类事件在付钱 ——
    print()
    print("按事件类别(看哪类操作让前缀失效、各付多少):")
    bp, bt = by_kind(plain), by_kind(tail)
    print(f"{'事件':>11} {'轮数':>5} {'plain上行':>11} {'tail上行':>11} "
          f"{'plain上传':>11} {'tail上传':>11} {'省':>7}")
    rows = []
    for kind in sorted(set(bp) | set(bt), key=lambda k: -bp.get(k, {}).get("json", 0)):
        a, b = bp.get(kind, {}), bt.get(kind, {})
        if not a or not b:
            continue
        ratio = a["wire"] / b["wire"] if b["wire"] else 0
        print(f"{kind:>11} {int(a['n']):>5} {_kb(a['wire']):>11} {_kb(b['wire']):>11} "
              f"{a['upload_ms']:>11.0f} {b['upload_ms']:>11.0f} {ratio:>6.1f}x")
        rows.append({"kind": kind, "turns": int(a["n"]),
                     "plain_bytes": a["wire"], "tail_bytes": b["wire"],
                     "plain_upload_ms": round(a["upload_ms"], 1),
                     "tail_upload_ms": round(b["upload_ms"], 1),
                     "ratio": round(ratio, 2)})
    print("(append 那一行是纯追加的最优情况;compact/branch 行就是前缀失效、"
          "要付一次全量的代价)")

    return {"rounds": n, "mbps": mbps, "trace_kinds": trace_kinds,
            "plain": plain, "tail": tail,
            "totals": {"json_bytes": pj, "plain_bytes": pw, "tail_bytes": tw,
                       "ratio": round(pw / tw, 2),
                       "plain_upload_s": round(pu / 1000, 3),
                       "tail_upload_s": round(tu / 1000, 3)},
            "by_kind": rows}


# ---------------------------------------------------------------------------
# 主流程
# ---------------------------------------------------------------------------

def main() -> None:
    ap = argparse.ArgumentParser(description="两个真 magpie 的真实场景对比")
    ap.add_argument("--magpie", required=True)
    ap.add_argument("--rounds", type=int, default=24)
    ap.add_argument("--mbps", type=float, default=25.0,
                    help="上行带宽(Mbit/s);0=不限速")
    ap.add_argument("--sessions", type=int, default=1)
    ap.add_argument("--compact-every", type=int, default=0)
    ap.add_argument("--branch-every", type=int, default=0)
    ap.add_argument("--tools-at", type=int, default=0)
    ap.add_argument("--turn-kb", type=int, default=8)
    ap.add_argument("--system-kb", type=int, default=0)
    ap.add_argument("--stream", action="store_true")
    ap.add_argument("--json-out", default="")
    ap.add_argument("--keep", action="store_true")
    args = ap.parse_args()
    if not os.access(args.magpie, os.X_OK):
        sys.exit(f"{args.magpie} 不可执行")

    root = tempfile.mkdtemp(prefix="magpie-realworld-")
    from tests.mock_backend import build_backend_app
    backend = build_backend_app()
    backend_port = _free_port()
    servers = [Server(backend, backend_port, "mock 推理服务(集群里的上游)")]
    cluster = Magpie(args.magpie, os.path.join(root, "cluster"),
                     f"127.0.0.1:{_free_port()}", tail=True)
    relay: Optional[ThrottledRelay] = None
    local: Optional[Magpie] = None
    try:
        print("magpie:", subprocess.run([args.magpie, "--version"],
              capture_output=True, text=True).stdout.strip())
        trace = build_trace(args.rounds, sessions=args.sessions,
                            compact_every=args.compact_every,
                            branch_every=args.branch_every,
                            tools_at=args.tools_at, turn_kb=args.turn_kb,
                            system_kb=args.system_kb)
        print(f"轨迹:{len(trace)} 轮,{trace.sessions} 个会话,事件 {trace.kinds()}")

        cluster.provider("mock", f"http://127.0.0.1:{backend_port}/v1",
                         "deepseek-chat", tail=False)
        cluster.start()
        cluster.wait()
        cluster_model = [m for m in cluster.models() if m.startswith("mock/")][0]

        relay = ThrottledRelay("127.0.0.1",
                               int(cluster.addr.rsplit(":", 1)[1]),
                               mbps=args.mbps)
        relay.start()
        peer = f"http://127.0.0.1:{relay.port}/v1"
        print(f"链路:{args.mbps or '不限速'} Mbps 上行,连接在 127.0.0.1:{relay.port}")

        local = Magpie(args.magpie, os.path.join(root, "local"),
                       f"127.0.0.1:{_free_port()}")
        local.provider("plain", peer, cluster_model, tail=False)
        local.provider("tail", peer, cluster_model, tail=True)
        local.start()
        local.wait()
        models = local.models()
        plain_model = [m for m in models if m.startswith("plain/")][0]
        tail_model = [m for m in models if m.startswith("tail/")][0]
        url = f"http://{local.addr}/v1/chat/completions"

        print("\n[1/2] plain…")
        plain = run_pass(url, plain_model, trace, relay, args.stream)
        print("[2/2] tail…")
        tail = run_pass(url, tail_model, trace, relay, args.stream)

        # 保真:两条路到推理服务的消息必须逐轮一致
        seen = backend.state.received[-(2 * len(trace)):]
        if len(seen) == 2 * len(trace):
            for i, (a, b) in enumerate(zip(seen[:len(trace)], seen[len(trace):])):
                if a["messages"] != b["messages"]:
                    raise AssertionError(f"第 {i + 1} 轮:两条路到达推理服务的消息不一致")
            print("\n两条路到达推理服务的 messages 逐轮一致 ✓")

        result = report(plain, tail, args.mbps, trace.kinds())
        if args.json_out:
            with open(args.json_out, "w") as f:
                json.dump(result, f, ensure_ascii=False, indent=2)
            print(f"已写 {args.json_out}")
    finally:
        for m in (local, cluster):
            if m is not None:
                m.stop()
        if relay is not None:
            relay.stop()
        for s in servers:
            s.stop()
        if not args.keep:
            shutil.rmtree(root, ignore_errors=True)


if __name__ == "__main__":
    main()
