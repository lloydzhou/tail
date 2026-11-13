"""网关服务端(FastAPI/ASGI):接收帧流,还原成 JSON,转发给上游。

启动::

    python -m tail.serve --backend https://api.deepseek.com --dbm ./blocks.db

客户端把 ``POST /v1/chat/completions`` 的 body 换成帧流、``Content-Type`` 设为
``application/x-tail-blocks`` 即可。响应(SSE 或 JSON)原样透传给客户端,
客户端不感知变化。

也接受普通 ``application/json`` 请求,原样转发——便于灰度对比和排障。
"""

from __future__ import annotations

import argparse
import json
import os
from typing import Any, Dict, Optional

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response, StreamingResponse

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
    app = FastAPI(title="Tail Gateway")
    app.state.storage = storage if storage is not None else make_storage("")
    app.state.gateway = gw.TailGateway(app.state.storage)
    app.state.backend = backend.rstrip("/")
    app.state.client = httpx.AsyncClient(timeout=timeout)

    @app.on_event("shutdown")
    async def _close() -> None:
        await app.state.client.aclose()

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
                                    "accept-encoding")}

    # ---- 走帧流:解码成文本,再反渲染成 JSON ----
    if gateway.wants_blocks(content_type):
        body = await request.body()
        result = gateway.decode_stream([body])
        text = b"".join(result)

        if result.missing:
            # 存储被清/重启/淘汰:让客户端下一轮重发全文
            return JSONResponse(
                status_code=409,
                content={"error": {"type": "tail_retransmit",
                                   "message": "缓存块缺失,请重发全文"}},
                headers={gw.HEADER_RETRANSMIT: "1"},
            )

        try:
            payload = render.unrender_request(text)
        except ValueError as exc:
            return JSONResponse(status_code=400,
                                content={"error": {"type": "tail_bad_frame",
                                                   "message": str(exc)}})
        body_out = json.dumps(payload, ensure_ascii=False).encode()
    else:
        body_out = await request.body()

    # ---- 转发上游 ----
    upstream_headers = dict(headers)
    upstream_headers["content-type"] = "application/json"
    req = client.build_request("POST", backend + path,
                               content=body_out, headers=upstream_headers)
    upstream = await client.send(req, stream=True)

    resp_headers = {k: v for k, v in upstream.headers.items()
                    if k.lower() not in ("content-length", "transfer-encoding",
                                         "content-encoding", "connection")}
    resp_headers["X-Tail-Status"] = result.status if gateway.wants_blocks(content_type) else "passthrough"
    resp_headers["X-Tail-Version"] = gw.TAIL_VERSION

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
