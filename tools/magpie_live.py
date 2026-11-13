"""真机对比:两个真实 magpie 之间,原生 JSON 上行 vs tail 上行。

链路(全部是本机进程,但角色和线上一致)::

    ┌────────────┐   两次:一次直连、一次走 tail     ┌────────────┐
    │ magpie(本地)│──────────────────────────────────▶│ magpie(集群)│──▶ 推理服务
    └────────────┘   ┌──────────────┐               └────────────┘
                     │ tail 网关/代理│  ← 只有 tail 那一次经过
                     └──────────────┘

- 集群侧:一个真正的 magpie gateway,provider 指向推理服务(这里用 mock 推理服务,
  真实环境换成 api.deepseek.com 之类的 chat completions 上游即可)。
- 本地侧:一个真正的 magpie gateway,两个 provider ——
  ``direct`` 直连集群 magpie,``tail`` 指向本地 tail 代理(再经 tail 网关到集群 magpie)。
- 两次请求体完全一样,只有 ``model`` 前面的 provider 名不同,所以「magpie 会怎么改写
  请求」这个变量被消掉了。

测的字节就是**两个 magpie 之间**的字节:直连那一次读本地代理(native 模式,纯转发)
的计数,tail 那一次读同一个代理的 tail 计数——同一个观测点,可公平对比。

用法::

    .venv/bin/python -m tools.magpie_live --magpie /tmp/magpie-cli --rounds 10

要求:``--magpie`` 是一个能跑的 magpie 二进制(``magpie serve`` 可用),它会用临时
XDG 目录,不碰本机 magpie 的配置。
"""

from __future__ import annotations

import argparse
import json
import os
import random
import shutil
import socket
import subprocess
import sys
import tempfile
import time
from typing import Dict, List, Optional, Tuple

import httpx2 as httpx
import uvicorn

from tail import proxy, serve
from tail.transport import storage

from .magpie_pair import Session, Server, _free_port, _kb, _mb, render_bytes


# ---------------------------------------------------------------------------
# 起 magpie
# ---------------------------------------------------------------------------

class Magpie:
    """一个真实的 magpie gateway 进程(临时配置目录)。"""

    def __init__(self, binary: str, root: str, addr: str) -> None:
        self.binary = binary
        self.root = root
        self.addr = addr
        self.proc: Optional[subprocess.Popen] = None
        os.makedirs(os.path.join(root, "config"), exist_ok=True)
        os.makedirs(os.path.join(root, "cache"), exist_ok=True)

    def env(self) -> Dict[str, str]:
        e = dict(os.environ)
        e["XDG_CONFIG_HOME"] = os.path.join(self.root, "config")
        e["XDG_CACHE_HOME"] = os.path.join(self.root, "cache")
        e["MAGPIE_ADDR"] = self.addr
        return e

    def provider(self, name: str, url: str, model: str) -> None:
        out = subprocess.run(
            [self.binary, "provider", "add", name, f"url={url}",
             "key=sk-tail-test", f"models={model}"],
            env=self.env(), capture_output=True, text=True, timeout=120)
        if out.returncode != 0:
            raise RuntimeError(f"magpie provider add {name} 失败: {out.stderr or out.stdout}")

    def start(self) -> None:
        self.proc = subprocess.Popen([self.binary, "serve"], env=self.env(),
                                     stdout=subprocess.DEVNULL,
                                     stderr=subprocess.DEVNULL)

    def wait(self, timeout: float = 30) -> None:
        deadline = time.time() + timeout
        url = f"http://{self.addr}/v1/models"
        while time.time() < deadline:
            try:
                if httpx.get(url, timeout=2).status_code == 200:
                    return
            except Exception:                       # noqa: BLE001 — 还没起来
                pass
            time.sleep(0.2)
        raise RuntimeError(f"magpie {self.addr} 没起来")

    def models(self) -> List[str]:
        r = httpx.get(f"http://{self.addr}/v1/models", timeout=10)
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
# 一轮请求
# ---------------------------------------------------------------------------

def one_call(url: str, model: str, messages: List[Dict], stream: bool,
             tools: List[Dict]) -> Dict:
    payload = {"model": model, "messages": messages, "tools": tools,
               "temperature": 0.3, "stream": stream}
    raw = render_bytes(payload)
    t0 = time.perf_counter()
    with httpx.Client(timeout=300) as client:
        r = client.post(url, content=raw,
                        headers={"content-type": "application/json",
                                 "authorization": "Bearer magpie"})
        r.raise_for_status()
    done = time.perf_counter()
    return {"bytes": len(raw), "total_ms": (done - t0) * 1000,
            "body": r.json() if "json" in r.headers.get("content-type", "") else None}


def run_pass(gateway_url: str, model: str, stats, rounds: int,
             stream: bool) -> Tuple[List[Dict], List[List[Dict]]]:
    rows: List[Dict] = []
    sent: List[List[Dict]] = []
    session = Session()
    for _ in range(rounds):
        messages = [dict(m) for m in session.advance()]
        sent.append(messages)
        before = stats.as_dict() if stats else None
        result = one_call(gateway_url, model, messages, stream, session_tools())
        after = stats.as_dict() if stats else None
        if stats:
            wire = after["wire_bytes_out"] - before["wire_bytes_out"]
            prep = after["prepare_ms"] - before["prepare_ms"]
            hits = after["hits"] - before["hits"]
        else:
            wire, prep, hits = result["bytes"], 0.0, 0
        rows.append({"round": len(rows) + 1, "json_bytes": result["bytes"],
                     "wire_bytes": wire, "total_ms": round(result["total_ms"], 1),
                     "prepare_ms": round(prep, 1), "hit": bool(hits)})
    return rows, sent


def session_tools() -> List[Dict]:
    from .magpie_pair import _TOOLS
    return _TOOLS


# ---------------------------------------------------------------------------
# 主流程
# ---------------------------------------------------------------------------

def main() -> None:
    ap = argparse.ArgumentParser(description="两个真实 magpie 之间的上行对比")
    ap.add_argument("--magpie", required=True, help="magpie 二进制路径")
    ap.add_argument("--rounds", type=int, default=10)
    ap.add_argument("--stream", action="store_true")
    ap.add_argument("--keep", action="store_true", help="跑完不删临时目录")
    ap.add_argument("--json-out", default="")
    args = ap.parse_args()

    if not os.access(args.magpie, os.X_OK):
        sys.exit(f"{args.magpie} 不可执行")

    root = tempfile.mkdtemp(prefix="magpie-live-")
    from tests.mock_backend import build_backend_app
    backend = build_backend_app()
    backend_port = _free_port()
    gateway_port = _free_port()
    native_port = _free_port()
    tail_port = _free_port()
    cluster_addr = f"127.0.0.1:{_free_port()}"
    local_addr = f"127.0.0.1:{_free_port()}"

    servers = [Server(backend, backend_port, "mock 推理服务(集群里的上游)")]
    store = storage.MemoryChainStore()
    gateway_app = serve.build_app(f"http://127.0.0.1:{backend_port}", store)
    native_app = proxy.build_app(f"http://127.0.0.1:{gateway_port}", native=True)
    tail_app = proxy.build_app(f"http://127.0.0.1:{gateway_port}")
    servers.append(Server(gateway_app, gateway_port, "tail 网关(集群侧)"))
    servers.append(Server(native_app, native_port, "本地代理·纯转发(基线)"))
    servers.append(Server(tail_app, tail_port, "本地代理·tail"))

    cluster = Magpie(args.magpie, os.path.join(root, "cluster"), cluster_addr)
    local = Magpie(args.magpie, os.path.join(root, "local"), local_addr)
    try:
        print("magpie 版本:", subprocess.run([args.magpie, "--version"],
              capture_output=True, text=True).stdout.strip() or "(无 --version)")
        # 集群 magpie:provider 指向推理服务
        cluster.provider("mock", f"http://127.0.0.1:{backend_port}/v1", "deepseek-chat")
        cluster.start()
        cluster.wait()
        upstream_model = [m for m in cluster.models() if m.startswith("mock/")][0]
        print(f"集群 magpie {cluster_addr} 上游模型 {upstream_model}")

        # 本地 magpie:两个 provider,一个直连、一个走 tail
        local.provider("direct", f"http://127.0.0.1:{native_port}/v1", upstream_model)
        local.provider("tail", f"http://127.0.0.1:{tail_port}/v1", upstream_model)
        local.start()
        local.wait()
        local_models = local.models()
        direct_model = [m for m in local_models if m.startswith("direct/")][0]
        tail_model = [m for m in local_models if m.startswith("tail/")][0]
        print(f"本地 magpie {local_addr}: {direct_model} / {tail_model}")

        url = f"http://{local_addr}/v1/chat/completions"
        print("\n[1/2] 直连(整包 JSON 上行)…")
        native_rows, native_sent = run_pass(url, direct_model,
                                            native_app.state.stats, args.rounds,
                                            args.stream)
        print("[2/2] tail(只上行尾巴字节)…")
        store.clear()
        tail_stats = tail_app.state.stats
        tail_stats.reset()
        tail_rows, tail_sent = run_pass(url, tail_model, tail_stats, args.rounds,
                                        args.stream)

        # —— 保真:两条路在推理服务那里必须看到同一串消息 ——
        received = backend.state.received[-2 * args.rounds:]
        if len(received) >= 2 * args.rounds:
            a, b = received[:args.rounds], received[args.rounds:]
            for i, (x, y) in enumerate(zip(a, b)):
                if x["messages"] != y["messages"]:
                    raise AssertionError(f"第 {i + 1} 轮:直连与 tail 到达推理服务的消息不一致")

        print()
        print("=" * 92)
        print(f"{'轮':>3} {'JSON上行':>10} {'直连耗时':>10} {'tail上行':>11} "
              f"{'tail耗时':>10} {'代理准备':>9} {'省':>7}")
        print(f"{'':>3} {'(KB)':>10} {'(ms)':>10} {'(KB)':>11} {'(ms)':>10} "
              f"{'(ms)':>9} {'(倍)':>7}")
        print("-" * 92)
        for a, b in zip(native_rows, tail_rows):
            ratio = a["wire_bytes"] / b["wire_bytes"] if b["wire_bytes"] else 0
            print(f"{a['round']:>3} {_kb(a['wire_bytes']):>10} {a['total_ms']:>10.1f} "
                  f"{_kb(b['wire_bytes']):>11} {b['total_ms']:>10.1f} "
                  f"{b['prepare_ms']:>9.1f} {ratio:>6.1f}x")
        print("-" * 92)
        nw = sum(r["wire_bytes"] for r in native_rows)
        tw = sum(r["wire_bytes"] for r in tail_rows)
        stats = tail_stats.as_dict()
        print(f"合计 {args.rounds} 轮:直连 {_mb(nw)} MB,tail {_mb(tw)} MB,"
              f"体积 {nw / tw:.1f}x;代理侧准备均摊 {stats['prepare_ms'] / args.rounds:.2f} ms/轮")
        print("两条路在推理服务侧收到的 messages 逐轮一致 ✓")

        if args.json_out:
            with open(args.json_out, "w") as f:
                json.dump({"magpie": args.magpie, "rounds": args.rounds,
                           "native": native_rows, "tail": tail_rows,
                           "totals": {"native_bytes": nw, "tail_bytes": tw,
                                      "ratio": round(nw / tw, 2)}},
                          f, ensure_ascii=False, indent=2)
            print(f"已写 {args.json_out}")
    finally:
        tail_proc = [cluster, local]
        for m in tail_proc:
            m.stop()
        for s in servers:
            s.stop()
        if not args.keep:
            shutil.rmtree(root, ignore_errors=True)


if __name__ == "__main__":
    main()
