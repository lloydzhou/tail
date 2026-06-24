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
from fastapi.responses import JSONResponse, StreamingResponse

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
        if cfg.debug and not cache_key:
            logger.info("[debug] 首次请求(无 cache_key),发完整 messages=%d 条 → 将建缓存", len(messages))
        if cache_key:
            meta = storage.get_meta(cache_key)
            consistent = (meta is not None
                          and declared_len is not None
                          and meta.get("len") == declared_len)
            if cfg.debug:
                logger.info(
                    "[debug] cache_key=%s meta_found=%s declared_len=%s meta_len=%s consistent=%s",
                    cache_key, meta is not None, declared_len,
                    meta.get("len") if meta else None, consistent)
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
                    if cfg.debug:
                        import json as _json
                        prefix_summary = [{"role": m.get("role"), "content": str(m.get("content", ""))[:40]} for m in prefix_msgs]
                        delta_summary = [{"role": m.get("role"), "content": str(m.get("content", ""))[:40]} for m in messages]
                        logger.info("[debug] ★ CACHE HIT — 从缓存还原前缀 %d 条 + 客户端增量 %d 条 = 转发后端 %d 条",
                                    len(prefix_msgs), len(messages), len(final_messages))
                        logger.info("[debug]   缓存的前缀(从 storage 还原): %s", _json.dumps(prefix_summary, ensure_ascii=False))
                        logger.info("[debug]   客户端发的增量: %s", _json.dumps(delta_summary, ensure_ascii=False))
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
                if cfg.debug:
                    logger.info("[debug] ✗ CACHE MISS — 原因: %s",
                                "meta 不存在" if meta is None else
                                ("未声明 prefix_length" if declared_len is None else
                                 f"len 不匹配(meta={meta.get('len')} vs declared={declared_len})"))
                if cfg.miss_mode != MISS_PASSTHROUGH:
                    return _fast_fail(cfg)

        # ---- 4. 透明代理转发(streaming 友好:用 stream 边收边发,不缓冲)----
        # 剥离:网关内部缓存头 + hop-by-hop 头 + host(后端按自己域名解析)
        #       + 空的 Authorization(客户端占位 key,避免 "Bearer " 非法值)
        _skip_req = {
            HEADER_CACHE_HASH.lower(), HEADER_CACHE_PREFIX_LENGTH.lower(),
            "host", "content-length", "transfer-encoding", "connection",
            "keep-alive", "te", "trailers", "upgrade",
        }
        fwd_headers = {}
        for k, v in request.headers.items():
            kl = k.lower()
            if kl in _skip_req:
                continue
            # 空的 Bearer(客户端占位 api_key)不透传,避免后端报 "Illegal header value b'Bearer '"
            if kl == "authorization":
                # 仅保留有实际 token 的 Authorization;"Bearer " / "Bearer" 视为空
                token_part = v.strip()
                if token_part.lower().startswith("bearer"):
                    token_part = token_part[6:].strip()  # 去 "Bearer" 前缀
                if not token_part:
                    continue  # 空 token,跳过
            fwd_headers[k] = v
        # 预算 cache_key(header_filter 等价,纯算无需网络)
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
            new_cache_key = ""

        client_kwargs: dict = {"timeout": httpx.Timeout(60.0)}
        if request.app.state.backend_transport is not None:
            client_kwargs["transport"] = request.app.state.backend_transport

        if cfg.debug:
            is_stream = bool(body.get("stream", False)) if body else False
            logger.info("[debug] → 转发后端 %s (stream=%s, messages=%d 条)",
                        f"{cfg.backend_url}/v1/chat/completions", is_stream, len(final_messages))
        try:
            # stream=True:不缓冲整个响应,SSE 字节流逐块透传
            client = httpx.AsyncClient(**client_kwargs)
            req = client.build_request(
                "POST", f"{cfg.backend_url}/v1/chat/completions",
                json=body if body else {}, headers=fwd_headers)
            upstream = await client.send(req, stream=True)
        except Exception as e:
            logger.exception("upstream forward failed")
            await client.aclose()
            return JSONResponse(status_code=502,
                                content={"error": {"message": f"upstream unavailable: {e}"}})

        # ---- 5. 异步写缓存(log_phase 等价;后端 2xx 才写)----
        if 200 <= upstream.status_code < 300:
            async def _write():
                try:
                    new_key = storage.put_request(request_snapshot, expire)
                    if cfg.debug:
                        logger.info("[debug] ✓ STORED — 缓存写入完成,新 cache_key=%s (system=%s, tools=%s, %d 条 messages)",
                                    new_key,
                                    "有" if request_snapshot.get("system") else "无",
                                    str(len(request_snapshot["tools"])) if request_snapshot.get("tools") else "无",
                                    len(request_snapshot.get("messages", [])))
                except Exception:
                    logger.exception("async put_request failed")
            try:
                asyncio.get_running_loop().create_task(_write())
            except RuntimeError:
                pass

        # ---- 6. 透明代理响应 + 注入缓存头 ----
        # 响应头:后端原样透传(text/event-stream 等保留)+ 追加 X-Cache-*
        # 剥离 hop-by-hop 头(否则客户端/代理混乱)
        skip = {"content-encoding", "transfer-encoding", "content-length", "connection"}
        resp_headers = {
            k: v for k, v in upstream.headers.items() if k.lower() not in skip
        }
        resp_headers[HEADER_RESP_CACHE_HASH] = new_cache_key
        resp_headers[HEADER_RESP_CACHE_EXPIRE] = str(expire)
        resp_headers[HEADER_RESP_CACHE_HIT] = "true" if hit else "false"

        async def stream_bytes():
            """逐块透传后端字节流(SSE 边收边发)。结束后关闭 client。"""
            try:
                async for chunk in upstream.aiter_raw():
                    yield chunk
            finally:
                await upstream.aclose()
                await client.aclose()

        return StreamingResponse(
            stream_bytes(),
            status_code=upstream.status_code,
            headers=resp_headers,
            media_type=upstream.headers.get("content-type"),
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
