"""握手自检:真的起三个进程,看两边是不是自己就把话问清楚了。

跑的是真实命令行,不是测试里的进程内 app:

    python -m tail.serve   ← 网关(进程内存储)
        ↑
    python -m tail.proxy   ← 本地代理
        ↑
    各种「对端」:正常 echo、只吃 JSON、假装支持再反悔

四段:

1. 网关宣告:``OPTIONS`` 在 chat 路径答 ``204 + X-Tail-Version + X-Tail-Block``,
   别的路径不答;
2. 正常链路:第 1 轮整包(还没问过)、握手落定后第 2 轮起换成尾巴,并且
   ``X-Tail-*`` 一个都不漏给客户端;
3. 只吃 JSON 的对端:一次都不试尾巴,只问一次,之后一直整包;
4. 假装支持又反悔的对端:整包重发一次把那一轮救回来,然后记成不支持。

用法:``python tools/handshake_live.py``(退出码非 0 表示有一项没过)
"""

from __future__ import annotations

import http.server
import json
import random
import socket
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Dict, List, Optional

ROOT = Path(__file__).resolve().parents[1]
PYTHON = sys.executable

#: 尾巴的 content-type 前缀,只看前缀就够判断「这一轮是不是发尾巴」
TAIL_CT = "application/x-tail-v3"

_FAILED: List[str] = []
_PROCS: List[subprocess.Popen] = []
_LOGS: List[Path] = []


# ---------------------------------------------------------------------------
# 「对端」:一个线程里的普通 HTTP 服务,分三种态度
# ---------------------------------------------------------------------------

def _completion(model: str, text: str = "ok") -> bytes:
    return json.dumps({
        "id": "chatcmpl-stub", "object": "chat.completion", "model": model,
        "choices": [{"index": 0, "finish_reason": "stop",
                     "message": {"role": "assistant", "content": text}}],
    }).encode()


class _Peer(http.server.BaseHTTPRequestHandler):
    """``mode`` 决定它怎么对待尾巴。"""

    mode = "echo"
    protocol_version = "HTTP/1.1"

    def log_message(self, *args):        # 别刷屏
        pass

    # -- 内部 --------------------------------------------------------------
    def _body(self) -> bytes:
        # 网关是流式转发重建后的 JSON 的,常常没有 content-length(用 chunked),
        # 这里两种都要读干净,否则连接会错位
        if "chunked" in (self.headers.get("transfer-encoding") or "").lower():
            out = b""
            while True:
                size = int(self.rfile.readline().split(b";")[0].strip() or b"0", 16)
                if not size:
                    self.rfile.readline()
                    return out
                out += self.rfile.read(size)
                self.rfile.read(2)
        length = int(self.headers.get("content-length") or 0)
        return self.rfile.read(length) if length else b""

    def _send(self, status: int, body: bytes, headers: Optional[Dict] = None):
        self.send_response(status)
        for k, v in (headers or {}).items():
            self.send_header(k, v)
        self.send_header("content-length", str(len(body)))
        self.send_header("content-type", "application/json")
        self.end_headers()
        self.wfile.write(body)

    def _is_tail(self) -> bool:
        return TAIL_CT in (self.headers.get("content-type") or "")

    # -- 路由 --------------------------------------------------------------
    def do_OPTIONS(self):                # noqa: N802 — http.server 的约定
        if self.mode in ("echo", "lying") and self._chat_path():
            self.send_response(204)
            self.send_header("allow", "POST, OPTIONS")
            self.send_header("x-tail-version", "3")
            self.send_header("x-tail-block", "512")
            self.send_header("content-length", "0")
            self.end_headers()
            return
        self.send_response(405)
        self.send_header("allow", "POST")
        self.send_header("content-length", "0")
        self.end_headers()

    def do_POST(self):                   # noqa: N802
        body = self._body()
        if self.mode == "json_only" and self._is_tail():
            return self._send(415, b'{"error":{"message":"unsupported media type"}}')
        if self.mode == "lying" and self._is_tail():
            return self._send(415, b'{"error":{"message":"unsupported media type"}}')
        try:
            payload = json.loads(body or b"{}")
        except ValueError:
            return self._send(400, b'{"error":{"message":"invalid json"}}')
        return self._send(200, _completion(payload.get("model", "m")))

    def _chat_path(self) -> bool:
        return self.path.rstrip("/").endswith("/chat/completions")


def _serve_peer(mode: str) -> int:
    """在一个后台线程里起对端,返回端口。"""
    port = _free_port()
    handler = type(f"Peer{mode}", (_Peer,), {"mode": mode})
    server = http.server.ThreadingHTTPServer(("127.0.0.1", port), handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return port


# ---------------------------------------------------------------------------
# 进程与 HTTP 小工具
# ---------------------------------------------------------------------------

def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _wait(port: int, tag: str, timeout: float = 20.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.3):
                return
        except OSError:
            time.sleep(0.05)
    raise RuntimeError(f"{tag} 没起来,看 {_LOGS[-1]}")


def _spawn(argv: List[str], tag: str) -> subprocess.Popen:
    log = Path(f"/tmp/tail-handshake-{tag}.log")
    _LOGS.append(log)
    fh = open(log, "w")
    proc = subprocess.Popen(argv, cwd=str(ROOT), stdout=fh, stderr=fh)
    _PROCS.append(proc)
    return proc


def _gateway(backend: str) -> int:
    port = _free_port()
    _spawn([PYTHON, "-m", "tail.serve", "--backend", backend, "--port", str(port),
            "--log-level", "warning"], "gateway")
    _wait(port, "tail.serve")
    return port


def _proxy(upstream: str, *flags: str, tag: str) -> int:
    port = _free_port()
    _spawn([PYTHON, "-m", "tail.proxy", "--upstream", upstream, "--port", str(port),
            "--log-level", "warning", *flags], tag)
    _wait(port, tag)
    return port


def _request(url: str, method: str = "GET", payload=None, headers=None):
    data = None
    hdrs = dict(headers or {})
    if payload is not None:
        data = json.dumps(payload).encode()
        hdrs.setdefault("content-type", "application/json")
    req = urllib.request.Request(url, data=data, method=method, headers=hdrs)
    try:
        with urllib.request.urlopen(req, timeout=120) as r:
            return r.status, dict(r.headers), r.read()
    except urllib.error.HTTPError as e:
        return e.code, dict(e.headers), e.read()


def _stats(port: int) -> Dict:
    return json.loads(_request(f"http://127.0.0.1:{port}/__tail/stats")[2])


def _chat(port: int, body: Dict):
    return _request(f"http://127.0.0.1:{port}/v1/chat/completions",
                    method="POST", payload=body)


# ---------------------------------------------------------------------------
# 请求体:一个 agent 形状的会话,逐轮变长
# ---------------------------------------------------------------------------

_WORDS = ("分析 数据 结果 模型 输入 输出 评估 指标 样本 训练 推理 上下文 "
          "缓存 传输 协议 分块 网关 客户端").split()
_TOOLS = [{"type": "function", "function": {
    "name": "read_file", "description": "读取一个文件",
    "parameters": {"type": "object", "properties": {"path": {"type": "string"}}}}}]


def _body(turns: int, rng) -> Dict:
    def text(n: int) -> str:
        return " ".join(rng.choice(_WORDS) for _ in range(n))

    messages = [{"role": "system", "content": "你是助手。" + text(900)}]
    for i in range(turns):
        messages.append({"role": "user", "content": f"q{i}: " + text(40)})
        messages.append({"role": "assistant", "content": f"a{i}: " + text(40)})
    return {"model": "deepseek-chat", "messages": messages, "stream": False,
            "tools": _TOOLS, "temperature": 0.7}


def _check(name: str, ok: bool, detail: str = "") -> None:
    print(("  ok   " if ok else "  FAIL ") + name + (f"   [{detail}]" if detail else ""))
    if not ok:
        _FAILED.append(name)


# ---------------------------------------------------------------------------
# 四段
# ---------------------------------------------------------------------------

def _advertisement(gateway: int) -> None:
    print("\n== 1. 网关宣告 ==")
    for path in ("/v1/chat/completions", "/chat/completions", "/v1/embeddings"):
        status, headers, _ = _request(f"http://127.0.0.1:{gateway}{path}",
                                      method="OPTIONS")
        print(f"  OPTIONS {path:22s} {status} "
              f"allow={headers.get('allow')!r} "
              f"ver={headers.get('x-tail-version')!r} "
              f"block={headers.get('x-tail-block')!r}")
        if path.endswith("embeddings"):
            _check("别的路径不广告", status == 405
                   and "x-tail-version" not in headers)
        else:
            _check("chat 路径宣告 v3 / 512",
                   status == 204 and headers.get("x-tail-version") == "3"
                   and headers.get("x-tail-block") == "512")


def _normal(tail: int, native: int) -> None:
    print("\n== 2. 正常链路(对端支持尾巴)==")
    rng = random.Random(5)
    body = _body(2, rng)
    before = _stats(tail)
    for turn in range(1, 5):
        status, headers, raw = _chat(tail, body)
        time.sleep(0.2)                       # 等后台那一问落定
        now = _stats(tail)
        wire = now["wire_bytes_out"] - before["wire_bytes_out"]
        json_in = now["json_bytes_in"] - before["json_bytes_in"]
        print(f"  第 {turn} 轮: {status} json={json_in:6d}B wire={wire:6d}B "
              f"plain={now['plain_requests']} tail={now['tail_requests']} "
              f"hit={now['hits']} miss={now['misses']} probes={now['probes']}")
        _check(f"第 {turn} 轮成功且不泄露 X-Tail-*",
               status == 200
               and not any(k.lower().startswith("x-tail-") for k in headers))
        if turn == 1:
            _check("没问过之前整包发(与 JSON 逐字节等长)", wire == json_in
                   and now["tail_requests"] == 0, f"{wire}/{json_in}")
        if turn == 2:
            _check("握手落定后换成尾巴", now["tail_requests"] == 1 and wire < json_in,
                   f"{wire}/{json_in}")
        if turn == 4:
            _check("第 3 轮起只剩尾巴", now["hits"] >= 2 and wire * 3 < json_in,
                   f"{wire}/{json_in}")
        before = now
        body["messages"] += [{"role": "user",
                              "content": f"继续{turn} " + "分析 数据 " * 30}]

    caps = _stats(tail)["capability"]
    _check("能力判断记成支持",
           list(caps.values()) == ["takes"], str(caps))
    _check("第 1 轮之后不再整包", _stats(tail)["plain_requests"] == 1)

    # --native 基线:同一网关、同一批请求、纯转发
    print("\n  --native 基线(同一网关,同一批请求)")
    rng = random.Random(5)
    body = _body(2, rng)
    for turn in range(1, 5):
        _chat(native, body)
        body["messages"] += [{"role": "user",
                              "content": f"继续{turn} " + "分析 数据 " * 30}]
    n, t = _stats(native), _stats(tail)
    print(f"  native: json={n['json_bytes_in']}B wire={n['wire_bytes_out']}B | "
          f"tail: json={t['json_bytes_in']}B wire={t['wire_bytes_out']}B")
    _check("tail 的总上行不到整包的一半",
           t["wire_bytes_out"] * 2 < n["wire_bytes_out"],
           f"{t['wire_bytes_out']}/{n['wire_bytes_out']}")


def _no_handshake_answer(mode: str, tag: str, *, flags: tuple = ()) -> None:
    peer = _serve_peer(mode)
    proxy = _proxy(f"http://127.0.0.1:{peer}", *flags, tag=tag)
    rng = random.Random(9)
    body = _body(2, rng)
    for turn in range(1, 4):
        status, headers, raw = _chat(proxy, body)
        time.sleep(0.2)
        now = _stats(proxy)
        print(f"  第 {turn} 轮: {status} plain={now['plain_requests']} "
              f"tail={now['tail_requests']} probes={now['probes']} "
              f"fallbacks={now['fallbacks']} caps={now['capability']}")
        _check(f"第 {turn} 轮仍然成功", status == 200 and b"ok" in raw)
        _check(f"第 {turn} 轮不泄露 X-Tail-*",
               not any(k.lower().startswith("x-tail-") for k in headers))
        body["messages"] += [{"role": "user", "content": f"再来{turn}"}]
    now = _stats(proxy)
    print(f"  合计: plain={now['plain_requests']} tail={now['tail_requests']} "
          f"probes={now['probes']} fallbacks={now['fallbacks']}")
    return now


def _peer_says_no() -> None:
    print("\n== 3. 对端只吃 JSON(答不了 OPTIONS)==")
    now = _no_handshake_answer("json_only", "proxy-json-only")
    _check("一次尾巴都没试", now["tail_requests"] == 0 and now["fallbacks"] == 0)
    _check("只问过一次", now["probes"] == 1)
    _check("一直整包", now["plain_requests"] == 3)
    _check("记成不支持", list(now["capability"].values()) == ["refuses"],
           str(now["capability"]))


def _peer_backpedals() -> None:
    print("\n== 4. 对端说支持、收到尾巴又 415 ==")
    now = _no_handshake_answer("lying", "proxy-lying")
    _check("只降级了一次(那一轮救回来了)", now["fallbacks"] == 1,
           str(now["fallbacks"]))
    _check("降级之后记成不支持", list(now["capability"].values()) == ["refuses"],
           str(now["capability"]))
    _check("之后不再试尾巴", now["tail_requests"] == 1, str(now["tail_requests"]))


def _blind() -> None:
    print("\n== 5. --no-handshake(旧行为:不问就发)==")
    now = _no_handshake_answer("json_only", "proxy-blind", flags=("--no-handshake",))
    _check("不问 OPTIONS", now["probes"] == 0)
    _check("靠 415 才发现,每轮降级一次", now["fallbacks"] == 3,
           str(now["fallbacks"]))


def main() -> int:
    echo = _serve_peer("echo")
    gateway = _gateway(f"http://127.0.0.1:{echo}")
    tail = _proxy(f"http://127.0.0.1:{gateway}", tag="proxy-tail")
    native = _proxy(f"http://127.0.0.1:{gateway}", "--native", tag="proxy-native")

    try:
        _advertisement(gateway)
        _normal(tail, native)
        _peer_says_no()
        _peer_backpedals()
        _blind()
    finally:
        for proc in _PROCS:
            proc.terminate()
        for proc in _PROCS:
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()

    if _FAILED:
        print(f"\n失败 {len(_FAILED)} 项: {_FAILED}")
        return 1
    print("\n全部通过")
    return 0


if __name__ == "__main__":
    sys.exit(main())
