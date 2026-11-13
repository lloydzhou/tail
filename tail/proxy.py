"""本地一侧的 tail 客户端代理:让 magpie 不改一行代码就走 tail transport。

部署形态(两个 magpie,中间走 tail)::

    magpie(本地)  ──JSON──▶  本代理  ──X-Tail-Have + 尾部字节──▶  tail 网关(集群)
                                                                    │  Redis 块缓存
                                                                    ▼
                                                              magpie(集群) ──▶ 推理服务

两端要改的都只是配置:

  - 本地 magpie:provider 的 base_url 指向本代理(默认 ``http://127.0.0.1:8766/v1``);
  - 集群 magpie:``python -m tail.serve --backend http://127.0.0.1:3425 --redis ...``
    放在它前面;网关把还原出来的标准 JSON 转发给 magpie,块缓存(Redis)在网关这一侧。

本代理做四件事:

1. 收下 magpie 发来的标准 JSON(magpie 本来就是整包发出去的);
2. ``TailClient.prepare()``:渲染文本、算父链、自检前缀,只把**新增的尾巴**发上线;
3. 响应(SSE 或 JSON)原样流回 magpie;``X-Tail-Have`` 只在本地记下,不给 magpie 看;
4. 收到 ``409 + X-Tail-Retransmit`` 时丢掉凭据、全量重发一次——对 magpie 完全透明。

``--native`` 模式:不做任何 tail 变换,只做纯转发。用来在同一台机器、同一条链路上
跑 A/B 基线(原生 JSON 上行 vs tail 上行)。

为什么不把这段直接写进 magpie:magpie 是 Go,入口请求体在中间件之前就被读完、出口没有
可插的 ``RoundTripper``,所以线缆层面的省必须改它的 Go 源码。本代理是同协议的参考实现,
也是真机取数的部署件;日后翻成 Go 时,行为以这里为准。
"""

from __future__ import annotations

import argparse
import json
import time
from typing import Any, AsyncIterator, Dict, Optional

import httpx2 as httpx
from fastapi import FastAPI, Request
from fastapi.responses import Response, StreamingResponse

from .transport import gateway as gw
from .transport.codec import PreparedRequest
from .transport.gateway import TailClient

#: 本代理在线上额外加的头的估算字节(X-Tail-Have 约 54 B + content-type 约 40 B),
#: 只用于统计「本代理发往集群的字节」。
_TAIL_HEADER_BYTES = 96

#: 请求侧不该透传的头(逐跳 / 由 httpx 重算)
_DROP = {
    "host", "content-length", "content-type", "transfer-encoding",
    "accept-encoding", "connection", "keep-alive", "proxy-connection",
    "te", "trailer", "upgrade",
}

#: 响应侧同样要丢掉的
_DROP_RESP = {"content-length", "transfer-encoding", "content-encoding",
              "connection", "keep-alive"}


def _is_chat(path: str, headers) -> bool:
    """只有 chat completions 走 tail;``/models`` 之类的原样透传。"""
    if not path.rstrip("/").endswith("/chat/completions"):
        return False
    return "json" in (headers.get("content-type") or "").lower()


def _fwd_headers(request: Request, content_type: str) -> Dict[str, str]:
    out = {k: v for k, v in request.headers.items() if k.lower() not in _DROP}
    out["content-type"] = content_type
    return out


class Stats:
    """两个 magpie 之间的字节账,``GET /__tail/stats`` 可以读走。"""

    def __init__(self) -> None:
        self.requests = 0
        self.tail_requests = 0
        self.native_requests = 0
        self.passthrough = 0
        self.json_bytes_in = 0      # magpie 发给本代理的
        self.wire_bytes_out = 0     # 本代理发往集群的(含固定头开销)
        self.prefix_bytes = 0       # 其中由服务端缓存命中、没有上线的
        self.hits = 0
        self.misses = 0
        self.retransmits = 0
        self.prepare_ms = 0.0     # 客户端侧「渲染 + 算块 + 自检前缀」累计耗时

    def reset(self) -> None:
        self.__init__()

    def as_dict(self) -> Dict[str, Any]:
        ratio = (self.json_bytes_in / self.wire_bytes_out
                 if self.wire_bytes_out else 0.0)
        return {
            "requests": self.requests,
            "tail_requests": self.tail_requests,
            "native_requests": self.native_requests,
            "passthrough": self.passthrough,
            "json_bytes_in": self.json_bytes_in,
            "wire_bytes_out": self.wire_bytes_out,
            "prefix_bytes_reused": self.prefix_bytes,
            "wire_saving_ratio": round(ratio, 3),
            "hits": self.hits,
            "misses": self.misses,
            "retransmits": self.retransmits,
            "prepare_ms": round(self.prepare_ms, 2),
        }


def build_app(upstream: str,
              native: bool = False,
              timeout: float = 600.0) -> FastAPI:
    """构造本地代理。

    Args:
      upstream: 集群侧 tail 网关的 base URL(例如 ``http://10.0.0.5:8765``)
      native: True 时不走 tail,纯转发 JSON(A/B 基线)
      timeout: 上游超时(秒)
    """
    app = FastAPI(title="Tail Client Proxy")
    app.state.upstream = upstream.rstrip("/")
    app.state.native = native
    app.state.tail = TailClient()
    app.state.stats = Stats()
    app.state.client = httpx.AsyncClient(timeout=timeout)

    # 内部端点先注册:catch-all 会吞掉它后面的所有路径
    @app.get("/__tail/stats")
    async def stats():
        return app.state.stats.as_dict()

    @app.get("/healthz")
    async def healthz():
        return {"ok": True, "upstream": app.state.upstream,
                "native": app.state.native}

    @app.api_route("/{path:path}", methods=["GET", "POST"])
    async def proxy(path: str, request: Request):
        return await _handle(request, "/" + path, app)

    return app


async def _handle(request: Request, path: str, app: FastAPI):
    client: httpx.AsyncClient = app.state.client
    stats: Stats = app.state.stats
    upstream: str = app.state.upstream
    tail: TailClient = app.state.tail
    stats.requests += 1

    raw = await request.body()

    # native 基线:纯转发;非 chat 路径也纯转发
    if app.state.native:
        stats.native_requests += 1
        stats.json_bytes_in += len(raw)
        stats.wire_bytes_out += len(raw)
        return await _forward_raw(client, upstream + path, request, raw)
    if not _is_chat(path, request.headers):
        stats.passthrough += 1
        return await _forward_raw(client, upstream + path, request, raw)

    payload = _loads(raw)
    if payload is None:
        # 不是合法 JSON:不猜,原样转发
        stats.passthrough += 1
        return await _forward_raw(client, upstream + path, request, raw)

    stats.tail_requests += 1
    stats.json_bytes_in += len(raw)
    return await _send_tail(client, tail, upstream + path, payload, stats)


def _loads(raw: bytes) -> Optional[Dict[str, Any]]:
    try:
        value = json.loads(raw)
    except ValueError:
        return None
    return value if isinstance(value, dict) else None


async def _forward_raw(client: httpx.AsyncClient, url: str,
                       request: Request, raw: bytes) -> Response:
    """非 chat 路径 / 非法 JSON / native 模式:一个字节都不改。"""
    headers = _fwd_headers(request, request.headers.get("content-type")
                           or "application/json")
    req = client.build_request(request.method, url, content=raw, headers=headers)
    upstream = await client.send(req, stream=True)
    return _relay(upstream)


async def _send_tail(client: httpx.AsyncClient, tail: TailClient, url: str,
                     payload: Dict[str, Any], stats: Stats) -> Response:
    """渲染 → 自检前缀 → 只发尾巴;409 时丢掉凭据重发一次。"""
    t0 = time.perf_counter()
    prepared = tail.prepare(payload)
    stats.prepare_ms += (time.perf_counter() - t0) * 1000
    upstream = await _post_tail(client, url, prepared, stats)

    if upstream.status_code == 409 and upstream.headers.get(gw.HEADER_RETRANSMIT):
        # 前缀失效(存储被清 / TTL 过期 / 重排):丢凭据,全量重发一次。
        await upstream.aclose()
        stats.retransmits += 1
        tail.invalidate()
        t0 = time.perf_counter()
        prepared = tail.prepare(payload)
        stats.prepare_ms += (time.perf_counter() - t0) * 1000
        upstream = await _post_tail(client, url, prepared, stats)

    if upstream.status_code != 409:
        tail.update_from_headers(prepared.session, upstream.headers)
        stats.prefix_bytes += prepared.reused_bytes
        if prepared.have_len:
            stats.hits += 1
        else:
            stats.misses += 1
    return _relay(upstream, hide_tail=True)


async def _post_tail(client: httpx.AsyncClient, url: str,
                     prepared: PreparedRequest, stats: Stats) -> httpx.Response:
    """发一次 tail 请求,返回**未读取**的响应(409 判定要在读之前)。"""
    headers = {"content-type": gw.CONTENT_TYPE_BLOCKS}
    have = gw.encode_have(prepared.have_len, prepared.have_tip)
    if have:
        headers[gw.HEADER_HAVE] = have

    sent = [0]

    async def body() -> AsyncIterator[bytes]:
        for chunk in prepared.body:
            if not chunk:
                continue
            sent[0] += len(chunk)
            yield chunk

    req = client.build_request("POST", url, content=body(), headers=headers)
    response = await client.send(req, stream=True)
    stats.wire_bytes_out += sent[0] + _TAIL_HEADER_BYTES
    return response


def _relay(upstream: httpx.Response, hide_tail: bool = False) -> Response:
    """把上游响应(SSE 或 JSON)原样流回 magpie。"""
    headers = {}
    for k, v in upstream.headers.items():
        low = k.lower()
        if low in _DROP_RESP:
            continue
        if hide_tail and low.startswith("x-tail-"):
            continue
        headers[k] = v
    return StreamingResponse(
        _relay_body(upstream),
        status_code=upstream.status_code,
        headers=headers,
        media_type=upstream.headers.get("content-type", "application/json"),
    )


async def _relay_body(upstream: httpx.Response) -> AsyncIterator[bytes]:
    try:
        async for chunk in upstream.aiter_bytes():
            if chunk:
                yield chunk
    finally:
        await upstream.aclose()


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(
        prog="python -m tail.proxy",
        description="本地 tail 客户端代理:magpie 的 base_url 指过来即可",
    )
    parser.add_argument("--upstream", required=True,
                        help="集群侧 tail 网关 base URL,例如 http://10.0.0.5:8765")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8766)
    parser.add_argument("--native", action="store_true",
                        help="不做 tail 变换,纯转发(A/B 基线)")
    parser.add_argument("--log-level", default="info")
    args = parser.parse_args(argv)

    app = build_app(args.upstream, native=args.native)
    import uvicorn
    uvicorn.run(app, host=args.host, port=args.port, log_level=args.log_level)


if __name__ == "__main__":
    main()
