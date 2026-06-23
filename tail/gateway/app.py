"""FastAPI 网关应用(对应 OpenResty gateway.lua 三阶段)。

单路由 POST /v1/chat/completions,三阶段在路由内同步完成:
  1. 读 header(必须在解析 body 前) + 命中判定 + 还原(等价 access_phase)
  2. httpx 转发后端 + 注入响应头(等价 header_filter_phase)
  3. asyncio.create_task 异步写缓存(等价 log_phase)
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Optional

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response

from . import protocol
from .protocol import (
    HEADER_CACHE_HASH, HEADER_CACHE_PREFIX_LENGTH, HEADER_RESP_CACHE_EXPIRE,
    HEADER_RESP_CACHE_HASH, HEADER_RESP_CACHE_HIT, MISS_PASSTHROUGH,
)
from .storage import Storage

logger = logging.getLogger(__name__)


def build_app(cfg: protocol.GatewayConfig, storage: Storage,
              backend_transport: Optional[httpx.BaseTransport] = None) -> FastAPI:
    """构建网关 FastAPI 应用。

    Args:
      cfg:     网关配置(backend_url / miss_mode / ttl 等)
      storage: 存储实例(DbmStorage / RedisStorage 等)
      backend_transport: 测试注入(ASGITransport);生产不传,走 cfg.backend_url
    """
    app = FastAPI(title="Tail Python Gateway")
    app.state.cfg = cfg
    app.state.storage = storage
    app.state.backend_transport = backend_transport

    @app.post("/v1/chat/completions")
    async def completions(request: Request) -> Response:
        # ---- 1. 读 header(必须在解析 body 前)----
        cache_key = request.headers.get(HEADER_CACHE_HASH)
        prefix_len_hdr = request.headers.get(HEADER_CACHE_PREFIX_LENGTH)
        declared_len = int(prefix_len_hdr) if prefix_len_hdr is not None else None

        # ---- 2. 解析 body ----
        try:
            body = await request.json()
        except Exception:
            body = {}
        messages = body.get("messages", []) if body else []
        system_val = body.get("system") if body else None
        tools_val = body.get("tools") if body else None
        final_messages = messages
        hit = False

        # ---- 3. 命中判定 + 还原(等价 access_phase)----
        if cache_key:
            meta = storage.get_meta(cache_key)
            consistent = (meta is not None
                          and declared_len is not None
                          and meta.get("len") == declared_len)
            if consistent:
                # 还原三段
                sys_val = storage.get_segment_field("sys", meta["sys_hash"])
                tls_val = storage.get_segment_field("tools", meta["tools_hash"])
                prefix_msgs = storage.reconstruct(meta["pfx_hash"])
                if prefix_msgs is None:
                    # 链断 → miss
                    if cfg.miss_mode != MISS_PASSTHROUGH:
                        return _fast_fail(cfg)
                else:
                    # 拼装完整 messages = prefix + 增量
                    final_messages = list(prefix_msgs) + list(messages)
                    hit = True
                    if body:
                        body["messages"] = final_messages
                        if sys_val is not None:
                            body["system"] = sys_val
                        if tls_val is not None:
                            try:
                                body["tools"] = json.loads(tls_val)
                            except (ValueError, TypeError):
                                pass
            else:
                # 未命中
                if cfg.miss_mode != MISS_PASSTHROUGH:
                    return _fast_fail(cfg)

        # ---- 4. 转发后端 ----
        # 构造转发 header:剥离内部缓存协商 header
        fwd_headers = {
            k: v for k, v in request.headers.items()
            if k.lower() not in (HEADER_CACHE_HASH.lower(),
                                 HEADER_CACHE_PREFIX_LENGTH.lower())
        }
        try:
            client_kwargs = {"timeout": httpx.Timeout(60.0)}
            if request.app.state.backend_transport is not None:
                client_kwargs["transport"] = request.app.state.backend_transport
            async with httpx.AsyncClient(**client_kwargs) as client:
                upstream = await client.post(
                    f"{cfg.backend_url}/v1/chat/completions",
                    json=body if body else {},
                    headers=fwd_headers,
                )
        except Exception as e:
            logger.exception("upstream forward failed")
            return JSONResponse(status_code=502,
                                content={"error": {"message": f"upstream unavailable: {e}"}})

        # ---- 5. 计算新 cache_key + 异步写(等价 header_filter + log_phase)----
        new_cache_key = ""
        expire = protocol.compute_expire(cfg.ttl, cfg.jitter)
        request_snapshot = {
            "system": system_val,
            "tools": tools_val,
            "messages": final_messages,
        }
        try:
            new_cache_key = storage.compute_cache_key(request_snapshot)
        except Exception:
            logger.exception("compute_cache_key failed")

        # 异步写(后端 2xx 才写)
        if upstream.status_code >= 200 and upstream.status_code < 300:
            async def _write():
                try:
                    storage.put_request(request_snapshot, expire)
                except Exception:
                    logger.exception("async put_request failed")
            try:
                asyncio.get_running_loop().create_task(_write())
            except RuntimeError:
                pass  # 无事件循环,跳过

        # ---- 6. 返回响应 + 注入缓存头 ----
        resp_headers = {
            HEADER_RESP_CACHE_HASH: new_cache_key,
            HEADER_RESP_CACHE_EXPIRE: str(expire),
            HEADER_RESP_CACHE_HIT: "true" if hit else "false",
        }
        # 保留后端响应的 content-type
        ct = upstream.headers.get("content-type")
        return Response(
            content=upstream.content,
            status_code=upstream.status_code,
            headers=resp_headers,
            media_type=ct,
        )

    @app.get("/__tail/health")
    async def health():
        return {"status": "ok", "storage": storage.ping()}

    @app.get("/__tail/stats")
    async def stats():
        return {"status": "ok"}

    return app


def _fast_fail(cfg: protocol.GatewayConfig) -> JSONResponse:
    """缓存未命中(fast_fail 模式)响应:422 + X-Cache-Hit: false。"""
    expire = protocol.compute_expire(cfg.ttl, cfg.jitter)
    return JSONResponse(
        status_code=422,
        headers={
            HEADER_RESP_CACHE_HASH: "",
            HEADER_RESP_CACHE_EXPIRE: str(expire),
            HEADER_RESP_CACHE_HIT: "false",
        },
        content={
            "error": {
                "message": "prefix cache miss; retry with full messages",
                "type": "prefix_cache_miss",
                "code": "cache_miss",
            }
        },
    )
