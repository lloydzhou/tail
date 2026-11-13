"""两个真实 magpie 之间的上行字节对比:tail 传输在 magpie 源码里,不经过任何旁路。

拓扑(两个 magpie 都是本仓库构建出的二进制)::

    ┌──────────────┐   provider   ┌──────────────┐        ┌──────────────┐
    │ agent/脚本    │─────────────▶│ magpie(本地) │        │ magpie(集群) │──▶ 推理服务
    └──────────────┘              └──────┬───────┘        └──────▲───────┘
                                         │  HTTP                 │
                                         └──▶ 计字节的 TCP 中继 ──┘

    本地 magpie 有两个 provider,指向同一个集群地址:
      plain —— 普通 chat completions 上游(整包 JSON 上行)
      tail  —— 同一个上游 + ``tail=on``(只上行尾巴字节)
    集群 magpie 的 gateway 开关 ``settings tail on`` 打开。

**测的就是两个 magpie 之间的字节**:TCP 中继逐字节计数,两条路走同一个中继、
同一个观测点、同一个上游、同一批请求体。

用法::

    .venv/bin/python -m tools.magpie_native --magpie /tmp/magpie-tail --rounds 10

``--magpie`` 必须是**带 tail 支持**的 magpie 二进制(``go build -tags nogui``)。
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
from typing import Dict, List, Optional, Tuple

import httpx2 as httpx
import uvicorn

from .magpie_pair import Session, Server, _free_port, _kb, _mb, render_bytes


# ---------------------------------------------------------------------------
# 计字节的 TCP 中继
# ---------------------------------------------------------------------------

class ByteRelay:
    """把一条 TCP 连接逐字节转发并计数:两个 magpie 之间真正上去的字节。"""

    def __init__(self, host: str, port: int) -> None:
        self.target = (host, port)
        self.up = 0      # 本地 magpie → 集群 magpie
        self.down = 0    # 回去
        self.port = _free_port()
        self._mu = threading.Lock()
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def _run(self) -> None:
        asyncio.set_event_loop(self._loop)
        self._loop.run_until_complete(
            asyncio.start_server(self._handle, "127.0.0.1", self.port))
        self._loop.run_forever()

    async def _handle(self, reader: asyncio.StreamReader,
                      writer: asyncio.StreamWriter) -> None:
        try:
            up_r, up_w = await asyncio.open_connection(*self.target)
        except OSError:
            writer.close()
            return

        async def pump(src, dst, upstream: bool) -> None:
            try:
                while True:
                    chunk = await src.read(65536)
                    if not chunk:
                        break
                    with self._mu:
                        if upstream:
                            self.up += len(chunk)
                        else:
                            self.down += len(chunk)
                    dst.write(chunk)
                    await dst.drain()
            except (ConnectionResetError, BrokenPipeError):
                pass
            finally:
                try:
                    dst.close()
                except Exception:               # noqa: BLE001 — 关掉就好
                    pass

        await asyncio.gather(pump(reader, up_w, True), pump(up_r, writer, False))

    def start(self) -> None:
        self._thread.start()
        deadline = time.time() + 10
        while time.time() < deadline:
            try:
                with socket.create_connection(("127.0.0.1", self.port), timeout=1):
                    return
            except OSError:
                time.sleep(0.02)
        raise RuntimeError("中继没起来")

    def reset(self) -> None:
        with self._mu:
            self.up, self.down = 0, 0

    def bytes_up(self) -> int:
        with self._mu:
            return self.up

    def stop(self) -> None:
        self._loop.call_soon_threadsafe(self._loop.stop)


# ---------------------------------------------------------------------------
# 两个 magpie 进程
# ---------------------------------------------------------------------------

class Magpie:
    def __init__(self, binary: str, root: str, addr: str, tail: bool = False) -> None:
        self.binary = binary
        self.root = root
        self.addr = addr
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

    def cli(self, *args: str) -> subprocess.CompletedProcess:
        return subprocess.run([self.binary, *args], env=self.env(),
                              capture_output=True, text=True, timeout=180)

    def provider(self, name: str, url: str, model: str, tail: bool) -> None:
        # 每个 provider 一个独立的 key:magpie 认为同一个 key 是同一个上游
        kv = [f"url={url}", f"key=sk-{name}-test", f"models={model}"]
        if tail:
            kv.append("tail=on")
        out = self.cli("provider", "add", name, *kv)
        if out.returncode != 0:
            raise RuntimeError(f"provider add {name}: {out.stderr or out.stdout}")

    def start(self) -> None:
        self.proc = subprocess.Popen([self.binary, "serve"], env=self.env(),
                                     stdout=subprocess.DEVNULL,
                                     stderr=subprocess.DEVNULL)

    def wait(self, timeout: float = 40) -> None:
        url = f"http://{self.addr}/v1/models"
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                if httpx.get(url, timeout=2).status_code == 200:
                    return
            except Exception:                   # noqa: BLE001 — 还没起来
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
# 一次会话
# ---------------------------------------------------------------------------

def one_call(url: str, model: str, messages: List[Dict], stream: bool) -> float:
    from .magpie_pair import _TOOLS
    payload = {"model": model, "messages": messages, "tools": _TOOLS,
               "temperature": 0.3, "stream": stream}
    t0 = time.perf_counter()
    with httpx.Client(timeout=300) as client:
        r = client.post(url, content=render_bytes(payload),
                        headers={"content-type": "application/json",
                                 "authorization": "Bearer magpie"})
        r.raise_for_status()
    return (time.perf_counter() - t0) * 1000


def run_pass(gateway_url: str, model: str, relay: ByteRelay, rounds: int,
             stream: bool) -> List[Dict]:
    rows: List[Dict] = []
    session = Session()
    relay.reset()
    for _ in range(rounds):
        messages = [dict(m) for m in session.advance()]
        before = relay.bytes_up()
        ms = one_call(gateway_url, model, messages, stream)
        rows.append({"round": len(rows) + 1, "json_bytes": len(render_bytes(
            {"model": model, "messages": messages})),
            "wire_bytes": relay.bytes_up() - before, "total_ms": round(ms, 1)})
    return rows


def main() -> None:
    ap = argparse.ArgumentParser(description="两个真实 magpie 之间的上行对比")
    ap.add_argument("--magpie", required=True, help="带 tail 支持的 magpie 二进制")
    ap.add_argument("--rounds", type=int, default=10)
    ap.add_argument("--stream", action="store_true")
    ap.add_argument("--keep", action="store_true")
    ap.add_argument("--json-out", default="")
    args = ap.parse_args()
    if not os.access(args.magpie, os.X_OK):
        sys.exit(f"{args.magpie} 不可执行")

    root = tempfile.mkdtemp(prefix="magpie-native-")
    from tests.mock_backend import build_backend_app
    backend = build_backend_app()
    backend_port = _free_port()
    servers = [Server(backend, backend_port, "mock 推理服务(集群里的上游)")]
    # 集群 magpie:MAGPIE_TAIL=1 打开入站开关(容器部署就是这么设的)
    cluster = Magpie(args.magpie, os.path.join(root, "cluster"),
                     f"127.0.0.1:{_free_port()}", tail=True)
    relay: Optional[ByteRelay] = None
    local: Optional[Magpie] = None
    try:
        ver = subprocess.run([args.magpie, "--version"], capture_output=True,
                             text=True).stdout.strip()
        print("magpie:", ver or "(无 --version)")

        # 集群 magpie:入站开关打开,provider 指向推理服务
        cluster.provider("mock", f"http://127.0.0.1:{backend_port}/v1",
                         "deepseek-chat", tail=False)
        cluster.start()
        cluster.wait()
        cluster_model = [m for m in cluster.models() if m.startswith("mock/")][0]
        print(f"集群 magpie {cluster.addr}: MAGPIE_TAIL=1, 上游 {cluster_model}")

        # 中继:本地 magpie 眼中「集群」就是中继的地址
        relay = ByteRelay("127.0.0.1", int(cluster.addr.rsplit(":", 1)[1]))
        relay.start()
        peer = f"http://127.0.0.1:{relay.port}/v1"

        # 本地 magpie:同一个地址,两条 provider,只差 tail=on
        local = Magpie(args.magpie, os.path.join(root, "local"),
                       f"127.0.0.1:{_free_port()}")
        local.provider("plain", peer, cluster_model, tail=False)
        local.provider("tail", peer, cluster_model, tail=True)
        local.start()
        local.wait()
        models = local.models()
        plain_model = [m for m in models if m.startswith("plain/")][0]
        tail_model = [m for m in models if m.startswith("tail/")][0]
        print(f"本地 magpie {local.addr}: {plain_model} / {tail_model}")

        url = f"http://{local.addr}/v1/chat/completions"
        print("\n[1/2] plain:普通 chat completions 上游(整包 JSON)…")
        plain = run_pass(url, plain_model, relay, args.rounds, args.stream)
        print("[2/2] tail:同一个上游 + tail=on…")
        tail = run_pass(url, tail_model, relay, args.rounds, args.stream)

        # 保真:两条路在推理服务那里必须看到同一串消息
        seen = backend.state.received[-(2 * args.rounds):]
        if len(seen) == 2 * args.rounds:
            for i, (a, b) in enumerate(zip(seen[:args.rounds], seen[args.rounds:])):
                if a["messages"] != b["messages"]:
                    raise AssertionError(f"第 {i + 1} 轮:两条路到达推理服务的消息不一致")

        print()
        print("=" * 88)
        print(f"{'轮':>3} {'plain上行':>11} {'plain耗时':>11} {'tail上行':>11} "
              f"{'tail耗时':>10} {'省':>7}")
        print(f"{'':>3} {'(KB)':>11} {'(ms)':>11} {'(KB)':>11} {'(ms)':>10} {'(倍)':>7}")
        print("-" * 88)
        for a, b in zip(plain, tail):
            ratio = a["wire_bytes"] / b["wire_bytes"] if b["wire_bytes"] else 0
            print(f"{a['round']:>3} {_kb(a['wire_bytes']):>11} {a['total_ms']:>11.1f} "
                  f"{_kb(b['wire_bytes']):>11} {b['total_ms']:>10.1f} {ratio:>6.1f}x")
        print("-" * 88)
        nw = sum(r["wire_bytes"] for r in plain)
        tw = sum(r["wire_bytes"] for r in tail)
        print(f"合计 {args.rounds} 轮(两个 magpie 之间的真实线上字节):"
              f" plain {_mb(nw)} MB,tail {_mb(tw)} MB,{nw / tw:.1f}x")
        print("两条路到达推理服务的 messages 逐轮一致 ✓")

        if args.json_out:
            with open(args.json_out, "w") as f:
                json.dump({"magpie": args.magpie, "rounds": args.rounds,
                           "plain": plain, "tail": tail,
                           "totals": {"plain_bytes": nw, "tail_bytes": tw,
                                      "ratio": round(nw / tw, 2)}},
                          f, ensure_ascii=False, indent=2)
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
