"""端到端基准:同一个请求体,原生 JSON 透传 vs Tail v3,比**整段时间**(wall clock)。

起两个真实端口:mock 上游、Tail 网关,客户端用 ``httpx2`` 打网关。
两种客户端唯一的差别是挂不挂 :class:`TailTransport`。

用法::

    python tools/e2e_bench.py

注意:测的是真实时间,机器有负载时数字会偏高。跑之前确认 ``uptime`` 的 load
接近核数;swap 用满会把进程换出,wall 能比 CPU 大一个数量级。
"""

from __future__ import annotations

import json
import random
import socket
import statistics
import sys
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Tuple

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import httpx2
import uvicorn

from tail import serve
from tail.transport import storage
from tail.transport.client import TailTransport

ALPHABET = "abcdefghijklmnopqrstuvwxyz0123456789 .,:;-_/()[]{}"


def text_of(nbytes: int, seed: int) -> str:
    return "".join(random.Random(seed).choices(ALPHABET, k=nbytes))


def build_payload(target_kb: int, seed: int = 1) -> Dict[str, Any]:
    messages = [{"role": "system", "content": text_of(8192, 0)}]
    total = 8192
    while total < target_kb * 1024:
        messages.append({"role": "user", "content": text_of(30000, seed)})
        messages.append({"role": "assistant", "content": text_of(30000, seed + 1)})
        total += 60000
        seed += 2
    return {"model": "deepseek-chat", "messages": messages}


def _free_port() -> int:
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


def _start(app, port: int) -> None:
    server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=port,
                                           log_level="error"))
    threading.Thread(target=server.run, daemon=True).start()


def start_servers() -> str:
    """启动 mock 上游 + Tail 网关,返回网关端点 URL。"""
    from tests.mock_backend import build_backend_app

    backend_port = _free_port()
    _start(build_backend_app(), backend_port)

    gateway_port = _free_port()
    _start(serve.build_app(f"http://127.0.0.1:{backend_port}",
                          storage.MemoryChainStore()), gateway_port)

    deadline = time.time() + 20
    while time.time() < deadline:
        try:
            socket.create_connection(("127.0.0.1", gateway_port), 0.2).close()
            break
        except OSError:
            time.sleep(0.05)
    return f"http://127.0.0.1:{gateway_port}/v1/chat/completions"


def measure(client, url: str, payload: dict, rounds: int = 7,
            warmup: int = 2) -> Tuple[float, dict]:
    """返回 (wall 中位数 ms, 最后一轮响应头)。预热让 Tail 走热。"""
    for _ in range(warmup):
        client.post(url, json=payload)
    walls: List[float] = []
    last = None
    for _ in range(rounds):
        start = time.perf_counter()
        last = client.post(url, json=payload)
        walls.append((time.perf_counter() - start) * 1000)
        if last.status_code != 200:
            raise RuntimeError(f"HTTP {last.status_code}: {last.text[:200]}")
    return statistics.median(walls), dict(last.headers)


def measure_turns(make_client, url: str, messages: list, turns: int = 8,
                  rounds: int = 3) -> Tuple[List[float], List[int]]:
    """多轮对话:每轮追加一条提问,量**每轮**的 wall 与上行字节。

    一次性客户端(透传)每轮都要重传整个 messages;Tail 只发增量。
    这是本项目要解决的场景,不是「同一份上下文重复提交」。
    """
    walls: List[float] = []
    sizes: List[int] = []
    # 同一个 client 贯穿全程:凭据在 transport 里,新建 client 等于丢整条前缀
    client = make_client()
    conv = list(messages)
    for turn in range(turns):
        # 真实的对话形态:每轮把新提问**追加**到 messages 上,上下文越滚越长
        conv = conv + [{"role": "user", "content": f"追问{turn}:" + text_of(400, 900 + turn)}]
        payload = {"model": "deepseek-chat", "messages": conv}
        samples = []
        for _ in range(rounds):
            start = time.perf_counter()
            r = client.post(url, json=payload)
            samples.append((time.perf_counter() - start) * 1000)
            if r.status_code != 200:
                raise RuntimeError(f"HTTP {r.status_code}: {r.text[:200]}")
        walls.append(statistics.median(samples))
        sizes.append(len(json.dumps(payload, ensure_ascii=False).encode()))
    return walls, sizes


class ThrottledTransport(httpx2.HTTPTransport):
    """按指定上行带宽给请求体加传输时延,模拟公网链路。

    本地回环没有带宽瓶颈,Tail 只是白白多花解码时间;真实公网相反。
    这个类用来还原真实链路,是「省字节 → 降 TTFT」这条因果链的唯一验证手段。
    """

    def __init__(self, uplink_mbps: float) -> None:
        super().__init__()
        self._bytes_per_sec = uplink_mbps * 1_000_000 / 8

    def handle_request(self, request):
        def slowed():
            for chunk in request.stream:
                yield chunk
                time.sleep(len(chunk) / self._bytes_per_sec)

        headers = [(k, v) for k, v in request.headers.raw
                   if k.lower() != b"content-length"]
        return super().handle_request(
            httpx2.Request(request.method, request.url, headers=headers,
                           content=slowed()))


def main() -> None:
    url = start_servers()
    sizes = (66, 242, 418, 829)

    print("### 回环:同一份上下文重复提交(wall clock,含两端与网络)\n")
    print("| 请求体 | 原生透传 | Tail(热) | 倍数 | Tail 状态 |")
    print("|---|---|---|---|---|")
    for kb in sizes:
        payload = build_payload(kb)
        raw = len(json.dumps(payload, ensure_ascii=False).encode())
        plain, _ = measure(httpx2.Client(), url, payload)
        tailed, headers = measure(
            httpx2.Client(transport=TailTransport()), url, payload)
        print(f"| {raw // 1024} KB | {plain:.1f} ms | {tailed:.1f} ms "
              f"| {tailed / plain:.1f}× | {headers.get('x-tail-status', '-')}"
              f" hit={headers.get('x-tail-hit', '-')} |")
    print()
    print("本地回环没有带宽瓶颈:Tail 要解码 + 反渲染 + 重建 JSON,所以更慢。")
    print("收益在上行字节,不在网关 CPU。下面两段才是真实场景。")
    print()

    print("### 慢上行:重传同一份上下文\n")
    print("模拟公网上行带宽,比较「整个请求体发完并被处理完」的时间。\n")
    print("| 上行带宽 | 原生 JSON 透传 | Tail(热) | 提速 |")
    print("|---|---|---|---|")
    for mbps in (10, 5, 2):
        payload = build_payload(829)
        plain, _ = measure(
            httpx2.Client(transport=ThrottledTransport(mbps)), url, payload, rounds=5)
        tailed, _ = measure(
            httpx2.Client(transport=TailTransport(inner=ThrottledTransport(mbps))),
            url, payload, rounds=5)
        print(f"| {mbps} Mbps | {plain:.0f} ms | {tailed:.0f} ms "
              f"| **{plain / tailed:.1f}×** |")
    print()

    print("### 多轮对话:每轮只多一条提问(真实场景)\n")
    print("从 829 KB 上下文起,连续 8 轮,每轮追加一条提问。")
    print("透传每轮重传整个 messages;Tail 只发新增的那一段。\n")
    context = build_payload(829)["messages"]
    plain_walls, sizes = measure_turns(
        lambda: httpx2.Client(transport=ThrottledTransport(5)), url, context)
    tail_walls, _ = measure_turns(
        lambda: httpx2.Client(
            transport=TailTransport(inner=ThrottledTransport(5))), url, context)
    print("5 Mbps 上行(约合 4G/家宽上行到公网的现实取值):\n")
    print("| 轮次 | 原生 JSON 体积 | 透传 | Tail | 提速 |")
    print("|---|---|---|---|---|")
    for i, (plain, tail) in enumerate(zip(plain_walls, tail_walls), 1):
        print(f"| 第 {i} 轮 | {sizes[i - 1]:,} B | {plain:.0f} ms "
              f"| {tail:.0f} ms | **{plain / tail:.1f}×** |")
    print()
    print("透传每轮都要把整份上下文推上去(1.46 s 是 888 KB / 5 Mbps 的物理下限);")
    print("Tail 只推新增的那条提问,剩下的是「网关解码整条前缀」的固定成本,与会话长度成正比。")
    print()


if __name__ == "__main__":
    main()
