"""网关服务端(FastAPI/ASGI):接收帧流,还原成 JSON,转发给上游。

启动::

    python -m tail.serve --backend https://api.deepseek.com --dbm ./blocks.db

客户端把 ``POST /v1/chat/completions`` 的 body 换成帧流、``Content-Type`` 设为
``application/x-tail-v2`` 即可。响应(SSE 或 JSON)原样透传给客户端,
客户端不感知变化。

也接受普通 ``application/json`` 请求,原样转发——便于灰度对比和排障。
"""

from __future__ import annotations

import argparse
import asyncio
import json
import queue
import os
from concurrent.futures import ThreadPoolExecutor
from typing import Any, AsyncIterator, Dict, Iterator, Optional

import httpx2 as httpx
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse

from .transport import gateway as gw
from .transport import render
from .transport.storage import make_storage


def build_app(backend: str,
              storage=None,
              timeout: float = 600.0) -> FastAPI:
    """构造网关 ASGI app。

    Args:
      backend: 上游 base URL,例如 ``https://api.deepseek.com``
      storage: 块存储实例;None 时用进程内内存存储
      timeout: 上游请求超时(秒)。流式响应下这是「读到第一块」的超时
    """
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        yield
        await app.state.client.aclose()

    app = FastAPI(title="Tail Gateway", lifespan=lifespan)
    app.state.storage = storage if storage is not None else make_storage("")
    app.state.gateway = gw.TailGateway(app.state.storage)
    app.state.backend = backend.rstrip("/")
    app.state.client = httpx.AsyncClient(timeout=timeout)

    @app.api_route("/v1/chat/completions", methods=["POST"])
    async def chat_completions(request: Request):
        return await _handle(request, "/v1/chat/completions", app)

    @app.api_route("/{path:path}", methods=["POST"])
    async def passthrough(path: str, request: Request):
        return await _handle(request, "/" + path, app)

    @app.get("/healthz")
    async def healthz():
        st = app.state.storage
        return {
            "ok": True,
            "storage": type(st).__name__,
            "blocks": len(st) if hasattr(st, "__len__") else None,
            "ping": st.ping() if hasattr(st, "ping") else None,
        }

    return app


async def _handle(request: Request, path: str, app: FastAPI):
    gateway: gw.TailGateway = app.state.gateway
    backend: str = app.state.backend
    client: httpx.AsyncClient = app.state.client

    content_type = request.headers.get("content-type", "")
    headers = {k: v for k, v in request.headers.items()
               if k.lower() not in ("host", "content-length", "content-type",
                                    "accept-encoding", "transfer-encoding")}
    upstream_headers = dict(headers)
    upstream_headers["content-type"] = "application/json"

    if not gateway.wants_blocks(content_type):
        # 普通 JSON:原样转发。同样**不先收齐**——直接透传请求体流,
        # 否则大 JSON 会在网关内存里整包落一次,再被拆成多个包发出去。
        # httpx 接受 AsyncIterable[bytes] 作为请求体,自动用 chunked 发。
        req = client.build_request("POST", backend + path,
                                   content=request.stream(),
                                   headers=upstream_headers)
        upstream = await client.send(req, stream=True)
        return _respond(upstream, "passthrough", None)

    # ---- 帧流路径 ----
    # **全程流式**:body 片段一到达就喂给解码器,解码器边解边产出 message,
    # JSON 片段边生成边交给 httpx 发给上游。
    #
    # 不能先 ``await request.body()`` 把整包收完——那样整份请求体already
    # 躺在内存里,后面的"流式"只是把一个大包切成多个小包发,上游仍要等
    # 最后一字节到齐才开始处理,完全没有意义。
    #
    # 内存峰值只与「单个块/单条 message」成正比,与请求体总大小无关。
    body = _AsyncToSyncBody(request)
    body.start()                      # 在事件循环线程里启动后台泵
    session = gateway.iter_payload_json(body)
    req = client.build_request("POST", backend + path,
                               content=_AsyncJsonStream(session),
                               headers=upstream_headers)
    upstream = await client.send(req, stream=True)
    return _respond(upstream, session.status, session)


class _AsyncToSyncBody:
    """把异步请求体转成**同步可迭代**,供同步解码器消费。

    解码器(渲染解析、切块、取块)是 CPU 密集的同步代码,而请求体是异步流。
    这里用一个**有界队列**桥接:

      - 后台协程持续 ``request.stream()``,把片段塞进队列;
      - 解码器在另一个线程里同步 ``next()`` 取片段。

    ``maxsize`` 提供背压:解码慢时队列会满,后台协程 await 阻塞,
    不会把整个 body 读进内存。这正是"边收边处理"的关键。

    内存占用 = 队列里最多 maxsize 个片段 + 单个块的缓冲,
    与请求体总大小无关。
    """

    def __init__(self, request, maxsize: int = 8) -> None:
        self._request = request
        self._queue: "queue.Queue[Optional[bytes]]" = queue.Queue(maxsize)
        self._task: Optional[asyncio.Task] = None
        # 后台泵的异常。必须显式记录并在消费端抛出:否则异常被 asyncio.Task
        # 捕获且无人 await,消费端只看到哨兵、当成正常结束,结果是**把被截断的
        # 请求静默转发给上游**,没有任何错误信号。
        self._error: Optional[BaseException] = None

    async def _pump(self) -> None:
        loop = asyncio.get_running_loop()
        try:
            async for chunk in self._request.stream():
                if chunk:
                    # queue 满时 put 会阻塞,但这是同步调用会卡住事件循环,
                    # 所以放到线程池里等,保持背压同时不堵 loop。
                    await loop.run_in_executor(None, self._queue.put, chunk)
        except BaseException as exc:          # noqa: BLE001 — 必须捕获后转交
            self._error = exc
        finally:
            await loop.run_in_executor(None, self._queue.put, None)   # 哨兵

    def start(self) -> None:
        """在**事件循环线程**里调用,启动后台泵。"""
        if self._task is None:
            self._task = asyncio.create_task(self._pump())

    def __iter__(self) -> Iterator[bytes]:
        # 注意:本方法可能在 executor 线程里被调用,不能在这里碰 asyncio。
        # 调用方必须先在事件循环线程里调用 start()。
        while True:
            chunk = self._queue.get()
            if chunk is None:
                # 哨兵:区分"正常结束"与"传输出错"
                if self._error is not None:
                    raise self._error
                return
            yield chunk


class _AsyncJsonStream(httpx.AsyncByteStream):
    """把同步的解码会话适配成 httpx 的异步流式请求体。

    解码是 CPU 密集的同步操作,放进线程池执行,避免阻塞事件循环;
    每产出一个片段就立刻交给 httpx 发出,不累积。
    """

    def __init__(self, session) -> None:
        self._session = session
        self._iter = iter(session)
        self._done = False
        # 单线程执行器:保证 next() 永远在同一个线程里调用。
        # 用默认的 None 会让多个 worker 线程并发调 next(),而惰性迭代器
        # 不是线程安全的——实测会丢消息(13 条只发出 2 条)。
        self._pool = ThreadPoolExecutor(max_workers=1,
                                        thread_name_prefix="tail-encode")

    async def __aiter__(self):
        loop = asyncio.get_running_loop()
        try:
            while not self._done:
                piece = await loop.run_in_executor(self._pool, self._next)
                if piece is None:
                    self._done = True
                    break
                yield piece
        finally:
            self._pool.shutdown(wait=False)

    def _next(self):
        try:
            return next(self._iter)
        except StopIteration:
            return None


def _respond(upstream: httpx.Response, status: Optional[str],
             session) -> StreamingResponse:
    """把上游响应原样流式转发(SSE 逐块透传)。"""
    resp_headers = {k: v for k, v in upstream.headers.items()
                    if k.lower() not in ("content-length", "transfer-encoding",
                                         "content-encoding", "connection")}
    if status:
        resp_headers[gw.HEADER_STATUS] = status
    resp_headers[gw.HEADER_VERSION] = gw.TAIL_VERSION
    if session is not None:
        resp_headers[gw.HEADER_HIT] = f"{session.hit_ratio:.3f}"
        if session.missing:
            # 有块取不到:告诉客户端下一轮重发全文
            resp_headers[gw.HEADER_RETRANSMIT] = "1"
    return StreamingResponse(
        _relay(upstream),
        status_code=upstream.status_code,
        headers=resp_headers,
        media_type=upstream.headers.get("content-type", "application/json"),
    )


async def _relay(upstream: httpx.Response):
    """把上游响应原样流式转发(SSE 逐块透传)。"""
    try:
        async for chunk in upstream.aiter_raw():
            yield chunk
    finally:
        await upstream.aclose()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main(argv: Optional[list] = None) -> None:
    parser = argparse.ArgumentParser(
        prog="python -m tail.serve",
        description="Tail 网关:接收帧流,还原成 JSON,转发给上游推理服务",
    )
    parser.add_argument("--backend", required=True,
                        help="上游 base URL,例如 https://api.deepseek.com")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--dbm", default="",
                        help="块存储路径(不填用进程内内存存储,重启即丢)")
    parser.add_argument("--redis", default="",
                        help="Redis URL,例如 redis://127.0.0.1:6379/0")
    parser.add_argument("--ttl", type=int, default=0,
                        help="块过期秒数(0 表示交给存储自身淘汰策略)")
    parser.add_argument("--log-level", default="info")
    args = parser.parse_args(argv)

    storage = make_storage(args.redis or (f"dbm:{args.dbm}" if args.dbm else ""),
                           ttl=args.ttl or None)
    app = build_app(args.backend, storage)
    import uvicorn
    uvicorn.run(app, host=args.host, port=args.port, log_level=args.log_level)


if __name__ == "__main__":
    main()
