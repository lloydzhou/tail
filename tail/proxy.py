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
3. 响应(SSE 或 JSON)原样流回上游客户端;``X-Tail-*`` 只在本地记下,不漏出去;
4. 收到 ``409 + X-Tail-Retransmit`` 时丢掉凭据、全量重发一次——对客户端完全透明;
5. **发尾巴之前先问一次**(``OPTIONS <chat 路径>``),因为尾巴不是 JSON:对端不说
   支持就一直整包发 JSON,而且这一问从不在请求路径上等(见 ``gateway.Capability``);
   对端说了支持却把尾巴当无关 JSON 拒掉时,整包重发一次并把它记成不支持。

为什么不把这段直接写进 magpie:magpie 是 Go,入口请求体在中间件之前就被读完、出口没有
可插的 ``RoundTripper``,所以线缆层面的省必须改它的 Go 源码。本代理是同协议的参考实现,
也是真机取数的部署件;日后翻成 Go 时,行为以这里为准。
"""

from __future__ import annotations

import argparse
import asyncio
import json
import re
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

#: 对端说了支持、却把尾巴当成「读不懂的 JSON」拒掉时,它的话里通常是这些
_NOT_JSON = re.compile(
    r"(invalid|expected|malformed|unexpected|parse[d]?|decode)[^.:;]{0,60}"
    r"(json|content[- ]?type|media[- ]?type)", re.I)

#: 握手给多久。超时就当「不支持」记下来,不让它每轮再等一次。
_PROBE_TIMEOUT = 3.0

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
    """这一侧自己的字节账,``GET /__tail/stats`` 可以读走。"""

    def __init__(self) -> None:
        self.requests = 0
        self.tail_requests = 0
        self.passthrough = 0
        self.json_bytes_in = 0      # magpie 发给本代理的
        self.wire_bytes_out = 0     # 本代理发往集群的(含固定头开销)
        self.prefix_bytes = 0       # 其中由服务端缓存命中、没有上线的
        self.hits = 0
        self.misses = 0
        self.retransmits = 0
        self.plain_requests = 0   # 这一轮整包发 JSON(还不知道对端支不支持 / 它不支持)
        self.fallbacks = 0        # 对端说了支持却拒了尾巴,整包重发
        self.probes = 0           # 后台问过多少次 OPTIONS
        self.prepare_ms = 0.0     # 客户端侧「渲染 + 算块 + 自检前缀」累计耗时

    def reset(self) -> None:
        self.__init__()

    def as_dict(self) -> Dict[str, Any]:
        ratio = (self.json_bytes_in / self.wire_bytes_out
                 if self.wire_bytes_out else 0.0)
        return {
            "requests": self.requests,
            "tail_requests": self.tail_requests,
            "passthrough": self.passthrough,
            "json_bytes_in": self.json_bytes_in,
            "wire_bytes_out": self.wire_bytes_out,
            "prefix_bytes_reused": self.prefix_bytes,
            "wire_saving_ratio": round(ratio, 3),
            "hits": self.hits,
            "misses": self.misses,
            "retransmits": self.retransmits,
            "plain_requests": self.plain_requests,
            "fallbacks": self.fallbacks,
            "probes": self.probes,
            "prepare_ms": round(self.prepare_ms, 2),
        }


def build_app(upstream: str, timeout: float = 600.0) -> FastAPI:
    """构造本地代理。

    Args:
      upstream: 对端 tail 网关的 base URL(例如 ``http://10.0.0.5:8765``)
      timeout: 上游超时(秒)
    """
    app = FastAPI(title="Tail Client Proxy")
    app.state.upstream = upstream.rstrip("/")
    app.state.tail = TailClient()
    app.state.stats = Stats()
    app.state.client = httpx.AsyncClient(timeout=timeout)
    app.state.caps = gw.Capability()
    app.state.probes: Dict[str, asyncio.Task] = {}

    # 内部端点先注册:catch-all 会吞掉它后面的所有路径
    @app.get("/__tail/stats")
    async def stats():
        return dict(app.state.stats.as_dict(),
                    capability=app.state.caps.snapshot())

    @app.get("/healthz")
    async def healthz():
        return {"ok": True, "upstream": app.state.upstream}

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

    if not _is_chat(path, request.headers):
        stats.passthrough += 1
        return await _forward_raw(client, upstream + path, request, raw)

    payload = _loads(raw)
    if payload is None:
        # 不是合法 JSON:不猜,原样转发
        stats.passthrough += 1
        return await _forward_raw(client, upstream + path, request, raw)

    url = upstream + path
    caps: gw.Capability = app.state.caps

    # 尾巴不是 JSON,所以先问对端接不接得住。三种状态里只有「支持」才发尾巴,
    # 而且这一问永远不在请求路径上等:不知道就整包发这一轮,问题挂到后台。
    if not caps.takes(url):
        if not caps.refuses(url):
            _ask_later(app, url)
        stats.plain_requests += 1
        stats.json_bytes_in += len(raw)
        stats.wire_bytes_out += len(raw)
        return await _forward_raw(client, url, request, raw)

    stats.tail_requests += 1
    stats.json_bytes_in += len(raw)
    return await _send_tail(client, tail, url, payload, stats,
                            app=app, request=request, raw=raw)


def _ask_later(app: FastAPI, url: str) -> None:
    """在后台问一次「这里收不收尾巴」。同一个 URL 只问一次。"""
    probes: Dict[str, asyncio.Task] = app.state.probes
    if url in probes and not probes[url].done():
        return
    probes[url] = asyncio.create_task(_probe(app, url))


async def _probe(app: FastAPI, url: str) -> None:
    """问对端:OPTIONS 请求自己要 POST 的那个 URL。"""
    client: httpx.AsyncClient = app.state.client
    caps: gw.Capability = app.state.caps
    stats: Stats = app.state.stats
    stats.probes += 1
    takes = False
    try:
        resp = await client.request("OPTIONS", url, timeout=_PROBE_TIMEOUT)
        takes = resp.status_code < 400 and gw.takes_blocks(resp.headers)
        await resp.aclose()
    except Exception:            # noqa: BLE001 — 没应答、超时、连接断了,都算不支持
        takes = False
    caps.set(url, takes)


def _loads(raw: bytes) -> Optional[Dict[str, Any]]:
    try:
        value = json.loads(raw)
    except ValueError:
        return None
    return value if isinstance(value, dict) else None


async def _forward_raw(client: httpx.AsyncClient, url: str,
                       request: Request, raw: bytes) -> Response:
    """非 chat 路径 / 非法 JSON / 还没问到答案:body 一个字节都不改。

    响应头照样过滤:``X-Tail-*`` 是两侧之间的内部头,不往上层漏。
    """
    headers = _fwd_headers(request, request.headers.get("content-type")
                           or "application/json")
    req = client.build_request(request.method, url, content=raw, headers=headers)
    upstream = await client.send(req, stream=True)
    return _relay(upstream, hide_tail=True)


async def _send_tail(client: httpx.AsyncClient, tail: TailClient, url: str,
                     payload: Dict[str, Any], stats: Stats,
                     app: FastAPI, request: Request, raw: bytes) -> Response:
    """渲染 → 自检前缀 → 只发尾巴;409 时丢掉凭据重发一次。

    对端**说了支持、却把尾巴当无关 JSON 拒掉**(运行中被换掉,或者中间有东西
    替它答了握手)时,整包 JSON 重发一次并把它记成不支持——这是传输自己的恢复,
    不能变成一个坏掉的轮次。
    """
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

    refused, buffered = await _refused_tail(upstream)
    if refused:
        await upstream.aclose()
        app.state.caps.set(url, False)
        stats.fallbacks += 1
        stats.plain_requests += 1
        stats.wire_bytes_out += len(raw)
        return await _forward_raw(client, url, request, raw)

    if upstream.status_code != 409:
        tail.update_from_headers(prepared.session, upstream.headers)
        stats.prefix_bytes += prepared.reused_bytes
        if prepared.have_len:
            stats.hits += 1
        else:
            stats.misses += 1
    return _relay(upstream, hide_tail=True, buffered=buffered)


async def _refused_tail(upstream: httpx.Response) -> tuple[bool, Optional[bytes]]:
    """对端是不是「说支持、却拒了尾巴」,以及已经读下来的响应体。

    只认两种情形:``415``(它的意思就是「不吃这个 content type」),或者
    ``400``/``422`` 且正文提到 json / content-type。真正的内容策略拒绝不会被误判,
    所以不会多打一次请求。其它状态的响应体也交回去(原样转发,一个字节不改)。
    """
    if upstream.status_code not in (400, 415, 422):
        return False, None
    body = await upstream.aread()
    await upstream.aclose()
    if upstream.status_code == 415:
        return True, body
    return bool(_NOT_JSON.search(body.decode("utf-8", "replace"))), body


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


def _relay(upstream: httpx.Response, hide_tail: bool = False,
           buffered: Optional[bytes] = None) -> Response:
    """把上游响应(SSE 或 JSON)原样流回 magpie。"""
    headers = {}
    for k, v in upstream.headers.items():
        low = k.lower()
        if low in _DROP_RESP:
            continue
        if hide_tail and low.startswith("x-tail-"):
            continue
        headers[k] = v
    if buffered is not None:
        # 判定要不要降级时已经把这段读下来了(都是错误响应,很小),原样交回;
        # content-type 已经在 headers 里,这里不再给 media_type
        return Response(content=buffered, status_code=upstream.status_code,
                        headers=headers)
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
    parser.add_argument("--log-level", default="info")
    args = parser.parse_args(argv)

    app = build_app(args.upstream)
    import uvicorn
    uvicorn.run(app, host=args.host, port=args.port, log_level=args.log_level)


if __name__ == "__main__":
    main()
