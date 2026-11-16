"""真机对比:真实的 magpie 经 Python 旁路代理时,上行省多少字节。

链路(全部是本机进程,但角色和线上一致)::

    ┌────────────┐         ┌──────────────┐        ┌────────────┐
    │ magpie(本地)│──JSON──▶│ tail 网关/代理│──尾巴──▶│ magpie(集群)│──▶ 推理服务
    └────────────┘         └──────────────┘        └────────────┘

- 集群侧:一个真正的 magpie gateway,provider 指向推理服务(这里用 mock 推理服务,
  真实环境换成 api.deepseek.com 之类的 chat completions 上游即可)。
- 两边的 magpie 都把内置插件 `tail` **关掉**(`magpie plugin builtin off tail`):
  tail 在这条链路里由 Python 的 `tail.serve` / `tail.proxy` 两个进程实现,
  magpie 自己的那套不参与,关掉才量得准。
- 本地侧:一个真正的 magpie gateway,provider 指向本地 tail 代理。

测的是代理这一侧的账:``json_bytes_in`` 是 magpie 发来的整包 JSON(不开这套传输时
就是它要上传的字节),``wire_bytes_out`` 是代理实际发出去的字节。同一批请求体上的
两个数,就是这套传输省下来的量——不需要为了取基线再跑一趟。

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
    """一个真实的 magpie gateway 进程(临时配置目录)。

    这里的 tail 由 Python 的 ``tail.serve`` / ``tail.proxy`` 实现,magpie 自己的
    内置插件 ``tail`` 只是要关掉 —— 它默认是开的,开着会去问对端 OPTIONS。
    """

    def __init__(self, binary: str, root: str, addr: str, tail: bool = False) -> None:
        self.binary = binary
        self.root = root
        self.addr = addr
        self.tail = tail
        self.proc: Optional[subprocess.Popen] = None
        os.makedirs(os.path.join(root, "config"), exist_ok=True)
        os.makedirs(os.path.join(root, "cache"), exist_ok=True)

    def env(self) -> Dict[str, str]:
        e = dict(os.environ)
        e["XDG_CONFIG_HOME"] = os.path.join(self.root, "config")
        e["XDG_CACHE_HOME"] = os.path.join(self.root, "cache")
        e["MAGPIE_ADDR"] = self.addr
        return e

    def switch(self, on: bool) -> None:
        """写内置插件 tail 的开关(这个工具里两边都关着)。"""
        out = subprocess.run(
            [self.binary, "plugin", "builtin", "on" if on else "off", "tail"],
            env=self.env(), capture_output=True, text=True, timeout=120)
        if out.returncode != 0:
            raise RuntimeError(
                f"magpie plugin builtin {'on' if on else 'off'} tail 失败: "
                f"{out.stderr or out.stdout}")
        self.tail = on

    def provider(self, name: str, url: str, model: str) -> None:
        out = subprocess.run(
            [self.binary, "provider", "add", name, f"url={url}",
             "key=sk-tail-test", f"models={model}"],
            env=self.env(), capture_output=True, text=True, timeout=120)
        if out.returncode != 0:
            raise RuntimeError(f"magpie provider add {name} 失败: {out.stderr or out.stdout}")

    def start(self) -> None:
        self.switch(self.tail)      # 起之前把开关写进 plugins.yml
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
            json_in = after["json_bytes_in"] - before["json_bytes_in"]
            prep = after["prepare_ms"] - before["prepare_ms"]
            hits = after["hits"] - before["hits"]
        else:
            wire, json_in, prep, hits = result["bytes"], result["bytes"], 0.0, 0
        rows.append({"round": len(rows) + 1, "json_bytes": json_in,
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
    tail_port = _free_port()
    cluster_addr = f"127.0.0.1:{_free_port()}"
    local_addr = f"127.0.0.1:{_free_port()}"

    servers = [Server(backend, backend_port, "mock 推理服务(集群里的上游)")]
    store = storage.MemoryChainStore()
    gateway_app = serve.build_app(f"http://127.0.0.1:{backend_port}", store)
    tail_app = proxy.build_app(f"http://127.0.0.1:{gateway_port}")
    servers.append(Server(gateway_app, gateway_port, "tail 网关(集群侧)"))
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

        # 本地 magpie:provider 指向本地代理
        local.provider("tail", f"http://127.0.0.1:{tail_port}/v1", upstream_model)
        local.start()
        local.wait()
        tail_model = [m for m in local.models() if m.startswith("tail/")][0]
        print(f"本地 magpie {local_addr}: {tail_model}")

        url = f"http://{local_addr}/v1/chat/completions"
        print("\n逐轮取字节…")
        store.clear()
        tail_stats = tail_app.state.stats
        tail_stats.reset()
        rows, sent = run_pass(url, tail_model, tail_stats, args.rounds, args.stream)

        # —— 保真:推理服务收到的 messages 必须是 magpie 发出来的那串 ——
        received = backend.state.received[-args.rounds:]
        if len(received) >= args.rounds:
            for i, (want, got) in enumerate(zip(sent, received)):
                if want != got["messages"]:
                    raise AssertionError(
                        f"第 {i + 1} 轮:推理服务收到的 messages 与发出来的不一致")

        print()
        print("=" * 78)
        print(f"{'轮':>3} {'整包 JSON':>12} {'tail 上行':>12} {'省':>7} "
              f"{'tail耗时':>10} {'代理准备':>9}")
        print(f"{'':>3} {'(KB)':>12} {'(KB)':>12} {'(倍)':>7} {'(ms)':>10} "
              f"{'(ms)':>9}")
        print("-" * 78)
        for r in rows:
            ratio = r["json_bytes"] / r["wire_bytes"] if r["wire_bytes"] else 0
            print(f"{r['round']:>3} {_kb(r['json_bytes']):>12} "
                  f"{_kb(r['wire_bytes']):>12} {ratio:>6.1f}x "
                  f"{r['total_ms']:>10.1f} {r['prepare_ms']:>9.1f}")
        print("-" * 78)
        total_json = sum(r["json_bytes"] for r in rows)
        total_wire = sum(r["wire_bytes"] for r in rows)
        stats = tail_stats.as_dict()
        print(f"合计 {args.rounds} 轮:整包 {_mb(total_json)} MB,tail "
              f"{_mb(total_wire)} MB,体积 {total_json / total_wire:.1f}x;"
              f"代理侧准备均摊 {stats['prepare_ms'] / args.rounds:.2f} ms/轮")
        print("推理服务侧收到的 messages 逐轮一致 ✓")

        if args.json_out:
            with open(args.json_out, "w") as f:
                json.dump({"magpie": args.magpie, "rounds": args.rounds,
                           "rows": rows,
                           "totals": {"json_bytes": total_json,
                                      "tail_bytes": total_wire,
                                      "ratio": round(total_json / total_wire, 2)}},
                          f, ensure_ascii=False, indent=2)
            print(f"已写 {args.json_out}")
    finally:
        for m in (cluster, local):
            m.stop()
        for s in servers:
            s.stop()
        if not args.keep:
            shutil.rmtree(root, ignore_errors=True)


if __name__ == "__main__":
    main()
