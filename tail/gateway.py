"""OpenAI 兼容网关 —— 缓存协商核心。

对应设计文档第 3 章「架构概览」、第 5 章「网关详细设计」。

协议语义(本实现采用的、清晰且可测的版本):

  「前缀」= 当前完整请求中、可作为下次复用的稳定头部。
  每次请求结束后,网关把**本次完整 messages** 作为新前缀缓存并返回新哈希,
  使前缀随对话单调增长,最大化复用率。

  - 首次:客户端发完整 messages M1 → 网关存 M1 → 返回 hash1,len=M1。
  - 后续:客户端发 hash1 + 增量 M2[len(M1):] → 网关命中后还原为 M1+增量=M2
    转发后端 → 再把 M2 作为新前缀缓存,返回 hash2,len=M2。

对设计文档第 5.3 节「未命中」分支的工程增强:
  乐观发送下,缓存未命中时客户端只发了增量,直接转发会给后端一个残缺请求。
  本实现默认 ``miss_mode="fast_fail"``:未命中时网关**不转发后端**,直接返回
  422 + ``X-Cache-Hit: false``,由 SDK 用完整 messages 重试一次(第 6.4 节)。
  这比「把增量当完整转发」更省资源、端到端更可靠;可用 ``miss_mode="passthrough"``
  退回到文档字面行为。
"""

from __future__ import annotations

import json
import logging
import time
from typing import Optional

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response

from . import protocol
from .hashing import compute_prefix_hash
from .store import CacheEntry, PrefixCacheStore
from .tokenizer import Tokenizer

logger = logging.getLogger(__name__)

# 网关内部使用的 header,转发后端前需移除(第 5.3 节)。
_INTERNAL_REQUEST_HEADERS = {
    protocol.HEADER_CACHE_HASH.lower(),
    protocol.HEADER_CACHE_PREFIX_LENGTH.lower(),
}

_DEFAULT_BACKEND_URL = "http://127.0.0.1:8000"


def build_app(
    store: PrefixCacheStore,
    tokenizer: Tokenizer,
    backend_url: str = _DEFAULT_BACKEND_URL,
    *,
    backend_transport: Optional[httpx.BaseTransport] = None,
    miss_mode: str = "fast_fail",
    cache_ttl: int = protocol.DEFAULT_CACHE_TTL_SECONDS,
    ttl_jitter: int = protocol.DEFAULT_TTL_JITTER_SECONDS,
) -> FastAPI:
    """构建网关 FastAPI 应用。

    Args:
      store:            前缀缓存存储。
      tokenizer:        分词器(用于算前缀哈希)。
      backend_url:      上游推理服务地址。
      backend_transport:httpx transport;测试时可注入 ASGITransport 实现进程内转发。
      miss_mode:        缓存未命中处理模式,"fast_fail"(默认) 或 "passthrough"。
      cache_ttl:        缓存 TTL(秒)。
      ttl_jitter:       TTL 随机抖动(秒),防雪崩。
    """
    if miss_mode not in ("fast_fail", "passthrough"):
        raise ValueError(f"unknown miss_mode: {miss_mode}")

    app = FastAPI(title="KV-Cache Gateway")
    app.state.store = store
    app.state.tokenizer = tokenizer
    app.state.backend_url = backend_url
    app.state.miss_mode = miss_mode
    app.state.cache_ttl = cache_ttl
    app.state.ttl_jitter = ttl_jitter
    # 可观测指标(第 Phase 3「监控」)。
    app.state.stats = {
        "requests": 0,
        "hits": 0,
        "misses": 0,
        "fast_fails": 0,
        "errors": 0,
        "bytes_from_client": 0,
        "bytes_to_backend": 0,
    }

    client_kwargs: dict = {"base_url": backend_url, "timeout": httpx.Timeout(60.0)}
    if backend_transport is not None:
        client_kwargs["transport"] = backend_transport
    http_client = httpx.AsyncClient(**client_kwargs)

    @app.on_event("shutdown")
    async def _shutdown() -> None:
        await http_client.aclose()

    async def _forward(method: str, path: str, *, json_body, headers) -> httpx.Response:
        return await http_client.request(method, path, json=json_body, headers=headers)

    @app.post("/v1/chat/completions")
    async def completions(request: Request) -> Response:
        app.state.stats["requests"] += 1
        raw_body = await request.body()
        app.state.stats["bytes_from_client"] += len(raw_body)
        try:
            body = json.loads(raw_body.decode("utf-8")) if raw_body else {}
        except Exception:
            body = {}

        messages = body.get("messages", []) or []
        model = body.get("model", "deepseek-chat")

        cache_hash = request.headers.get(protocol.HEADER_CACHE_HASH)
        prefix_len_hdr = request.headers.get(protocol.HEADER_CACHE_PREFIX_LENGTH)

        # 转发后端的请求头:剥离内部缓存协商 header。
        fwd_headers = {
            k: v
            for k, v in request.headers.items()
            if k.lower() not in _INTERNAL_REQUEST_HEADERS
        }

        hit = False
        forward_body = body
        new_prefix_messages = messages  # 默认:把本次收到的 messages 作为新前缀

        try:
            if cache_hash:
                declared_len = (
                    int(prefix_len_hdr) if prefix_len_hdr is not None else None
                )
                entry = store.get(cache_hash)
                consistent = (
                    entry is not None
                    and entry.model == model
                    and declared_len is not None
                    and entry.prefix_length == declared_len
                )
                if consistent:
                    # 命中:缓存前缀 + 增量 → 完整 messages(第 5.3 节命中分支)。
                    full_messages = list(entry.messages) + list(messages)
                    forward_body = {**body, "messages": full_messages}
                    new_prefix_messages = full_messages
                    hit = True
                    app.state.stats["hits"] += 1
                else:
                    # 未命中 / 过期 / 不一致。
                    app.state.stats["misses"] += 1
                    if app.state.miss_mode == "fast_fail":
                        app.state.stats["fast_fails"] += 1
                        return _miss_response(model, app.state.cache_ttl, app.state.ttl_jitter)
                    # passthrough:把当前 messages 当完整请求转发(文档字面行为)。
            else:
                # 无哈希:完整请求透传(第 5.3 节第 2 点)。
                app.state.stats["misses"] += 1

            # 转发后端。
            fwd_bytes = len(json.dumps(forward_body).encode("utf-8"))
            app.state.stats["bytes_to_backend"] += fwd_bytes
            upstream = await _forward(
                "POST",
                "/v1/chat/completions",
                json_body=forward_body,
                headers=fwd_headers,
            )
        except Exception:
            # 任何内部异常都不应让请求失败:降级为透传原始请求(第 5.4 节)。
            logger.exception("gateway internal error, falling back to passthrough")
            app.state.stats["errors"] += 1
            try:
                upstream = await _forward(
                    "POST",
                    "/v1/chat/completions",
                    json_body=body,
                    headers=fwd_headers,
                )
                hit = False
                new_prefix_messages = messages
            except Exception:
                return JSONResponse(
                    status_code=502,
                    content={"error": {"message": "upstream unavailable"}},
                )

        # 计算新前缀哈希并写入缓存(无论命中与否,都刷新前缀缓存)。
        now = time.time()
        token_ids = tokenizer.encode_messages(new_prefix_messages)
        new_hash = compute_prefix_hash(token_ids)
        expire = protocol.compute_expire(now=now, ttl=app.state.cache_ttl, jitter=app.state.ttl_jitter)
        store.set(
            CacheEntry(
                hash=new_hash,
                token_ids=token_ids,
                messages=list(new_prefix_messages),
                model=model,
                prefix_length=len(new_prefix_messages),
                created_at=now,
                expire_at=expire,
            )
        )

        # 构造响应:原样透传后端 body + 附加缓存协商 header。
        resp_headers = {
            **{k: v for k, v in upstream.headers.items() if k.lower() not in ("content-encoding", "transfer-encoding", "content-length")},
            protocol.HEADER_RESP_CACHE_HASH: new_hash,
            protocol.HEADER_RESP_CACHE_EXPIRE: str(expire),
            protocol.HEADER_RESP_CACHE_HIT: protocol.bool_header(hit),
        }
        return Response(
            content=upstream.content,
            status_code=upstream.status_code,
            headers=resp_headers,
            media_type=upstream.headers.get("content-type"),
        )

    @app.get("/__gateway/stats")
    async def stats() -> dict:
        s = app.state.stats
        saved = max(0, s["bytes_from_client"] - s["bytes_to_backend"])
        return {
            **s,
            "bytes_saved": saved,
            "cache_size": len(store),
        }

    @app.post("/__gateway/reset")
    async def reset() -> dict:
        """清空缓存与统计计数。"""
        store.clear()
        for k in app.state.stats:
            app.state.stats[k] = 0
        return {"ok": True}

    @app.post("/__gateway/clear-cache")
    async def clear_cache() -> dict:
        """仅清空缓存,保留统计计数(运维清缓存时不想丢监控数据)。"""
        store.clear()
        return {"ok": True, "cache_size": len(store)}

    return app


def _miss_response(model: str, ttl: int, jitter: int) -> JSONResponse:
    """fast_fail 模式下,缓存未命中时返回的快速失败响应。

    携带新的(空)缓存元信息提示客户端重置;客户端应改用完整 messages 重试。
    """
    now = time.time()
    expire = protocol.compute_expire(now=now, ttl=ttl, jitter=jitter)
    return JSONResponse(
        status_code=422,
        headers={
            protocol.HEADER_RESP_CACHE_HASH: "",
            protocol.HEADER_RESP_CACHE_EXPIRE: str(expire),
            protocol.HEADER_RESP_CACHE_HIT: protocol.bool_header(False),
        },
        content={
            "error": {
                "message": "prefix cache miss; retry with full messages",
                "type": "prefix_cache_miss",
                "code": "cache_miss",
            }
        },
    )
