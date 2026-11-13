"""端到端基准:同一个请求体,原生 JSON 透传 vs Tail 帧流,比**整段时间**(wall clock)。

起三个真实端口:mock 上游、Tail 网关,客户端用 `httpx2` 打网关。
两种客户端唯一的差别是挂不挂 `TailTransport`。

用法::

    python tools/e2e_bench.py

注意:测的是真实时间,机器有负载时数字会偏高。跑之前确认 `uptime` 的 load
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
from typing import Any, Dict, List

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import httpx2
import uvicorn

from tail import serve
from tail.transport import storage
from tail.transport.client import TailTransport
from tail.transport.gateway import TailClient

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


def start_servers():
    """启动 mock 上游 + Tail 网关,返回网关 URL。"""
    from tests.mock_backend import build_backend_app

    backend_port = _free_port()
    _start(build_backend_app(), backend_port)

    gateway_port = _free_port()
    _start(serve.build_app(f"http://127.0.0.1:{backend_port}",
                          storage.MemoryStorage()), gateway_port)

    deadline = time.time() + 20
    while time.time() < deadline:
        try:
            socket.create_connection(("127.0.0.1", gateway_port), 0.2).close()
            break
        except OSError:
            time.sleep(0.05)
    return f"http://127.0.0.1:{gateway_port}/v1/chat/completions"


def measure(client, url: str, payload: dict, rounds: int = 9) -> tuple:
    """返回 (wall 中位数 ms, 状态)。前两轮预热(让 Tail 走热)。"""
    body_size = len(json.dumps(payload, ensure_ascii=False).encode())
    for _ in range(2):
        client.post(url, json=payload)
    walls: List[float] = []
    last = None
    for _ in range(rounds):
        start = time.perf_counter()
        last = client.post(url, json=payload)
        walls.append((time.perf_counter() - start) * 1000)
        if last.status_code != 200:
            raise RuntimeError(f"HTTP {last.status_code}: {last.text[:200]}")
    status = last.headers.get("x-tail-status", "-")
    return statistics.median(walls), body_size, status


def main() -> None:
    url = start_servers()
    sizes = (66, 242, 418, 829)

    print("### 端到端耗时(wall clock,含两端与网络)\n")
    print("| 请求体 | 公网上行(实测) | 原生透传 | Tail 帧流(热) | 倍数 |")
    print("|---|---|---|---|---|")
    for kb in sizes:
        payload = build_payload(kb)
        # 客户端实际要发的字节数(热,即全部命中的稳态)
        encoder = TailClient()
        encoder.encode_request(payload)
        wire = len(encoder.encode_request(payload))
        plain, size, _ = measure(httpx2.Client(), url, payload)
        tailed, _, status = measure(
            httpx2.Client(transport=TailTransport()), url, payload)
        print(f"| {size // 1024} KB | {wire:,} B(省 {100 * (1 - wire / size):.0f}%) "
              f"| {plain:.1f} ms | {tailed:.1f} ms | {tailed / plain:.1f}× |")
    print()
    print("注:透传只是把字节搬过去;帧流要解码 + 反渲染 + 重建 JSON,")
    print("所以耗时高于透传。收益在上行字节,不在网关 CPU。")
    print()


if __name__ == "__main__":
    main()
