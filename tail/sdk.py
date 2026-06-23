"""带前缀缓存协商的客户端 SDK。

对应设计文档第 6 章「Client SDK 设计」。

设计目标:对调用方**完全透明**——调用方式与官方 openai Python SDK 一致,
内部自动维护 (model, base_url) -> 缓存 的映射、自动切分前缀/增量、自动降级。

提供两个等价外壳:
  - ``CachedClient``       :同步(基于 ``httpx.Client``),适合普通脚本/服务。
  - ``AsyncCachedClient``  :异步(基于 ``httpx.AsyncClient``),适合 async 框架,
    也支持以 ``httpx.ASGITransport`` 直连网关 ASGI app(用于测试/进程内调用)。
两者共享同一套协商逻辑(``_negotiate`` / ``_update_cache``)。
"""

from __future__ import annotations

import asyncio
import threading
import time
from dataclasses import dataclass, field
from typing import List, Optional, Tuple

import httpx

from . import protocol
from .hashing import compute_prefix_hash
from .tokenizer import Tokenizer


@dataclass
class _CacheState:
    """(model, base_url) 维度的本地缓存(第 6.2 节)。"""

    cache_hash: str = ""
    expire_time: float = 0.0
    prefix_messages: List[dict] = field(default_factory=list)

    def is_valid(self, now: float | None = None) -> bool:
        if not self.cache_hash:
            return False
        if now is None:
            now = time.time()
        return now < self.expire_time


# ---------------------------------------------------------------------------
# 共享的协商逻辑(与 httpx 客户端类型无关)
# ---------------------------------------------------------------------------


def _build_send_args(
    *,
    model: str,
    messages: List[dict],
    extra: dict,
    cache_hash: str = "",
    prefix_length: Optional[int] = None,
    api_key: str,
) -> Tuple[dict, dict]:
    """构造请求体与头部。"""
    body: dict = {"model": model, "messages": messages, **extra}
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    if cache_hash:
        headers[protocol.HEADER_CACHE_HASH] = cache_hash
        if prefix_length is not None:
            headers[protocol.HEADER_CACHE_PREFIX_LENGTH] = str(prefix_length)
    return body, headers


class _CacheManager:
    """线程安全 + 异步安全的本地缓存管理器。

    异步外壳下用 ``asyncio.Lock`` 保证协程间互斥;同步外壳下用 ``threading.Lock``。
    两套锁互不干扰,但同一实例不应混用同步/异步调用。
    """

    def __init__(self):
        self._thread_lock = threading.Lock()
        self._async_lock: Optional[asyncio.Lock] = None
        self._caches: dict[tuple[str, str], _CacheState] = {}

    def _get_async_lock(self) -> asyncio.Lock:
        if self._async_lock is None:
            self._async_lock = asyncio.Lock()
        return self._async_lock

    # ---- 同步路径 ----
    def decide_sync(self, key, messages):
        with self._thread_lock:
            state = self._caches.get(key)
            valid = state is not None and state.is_valid()
            return (state, valid)

    def update_sync(self, key, sent_messages, resp_headers):
        self._update(key, sent_messages, resp_headers, self._thread_lock)

    def invalidate_sync(self, key):
        with self._thread_lock:
            self._caches.pop(key, None)

    def get_state_sync(self, key) -> Optional[_CacheState]:
        with self._thread_lock:
            return self._caches.get(key)

    # ---- 异步路径 ----
    async def decide_async(self, key, messages):
        async with self._get_async_lock():
            state = self._caches.get(key)
            valid = state is not None and state.is_valid()
            return (state, valid)

    async def update_async(self, key, sent_messages, resp_headers):
        lock = self._get_async_lock()
        async with lock:
            self._update_locked(key, sent_messages, resp_headers)

    async def invalidate_async(self, key):
        async with self._get_async_lock():
            self._caches.pop(key, None)

    async def get_state_async(self, key) -> Optional[_CacheState]:
        async with self._get_async_lock():
            return self._caches.get(key)

    # ---- 共享更新逻辑 ----
    def _update(self, key, sent_messages, resp_headers, lock):
        with lock:
            self._update_locked(key, sent_messages, resp_headers)

    def _update_locked(self, key, sent_messages, resp_headers):
        new_hash = resp_headers.get(protocol.HEADER_RESP_CACHE_HASH)
        expire_str = resp_headers.get(protocol.HEADER_RESP_CACHE_EXPIRE)
        if not new_hash:
            self._caches.pop(key, None)
            return
        try:
            expire = int(expire_str) if expire_str else time.time()
        except ValueError:
            expire = time.time()
        state = self._caches.get(key)
        hit = protocol.parse_cache_hit(resp_headers.get(protocol.HEADER_RESP_CACHE_HIT))
        if hit and state is not None:
            prefix_messages = list(state.prefix_messages) + list(sent_messages)
        else:
            prefix_messages = list(sent_messages)
        self._caches[key] = _CacheState(
            cache_hash=new_hash,
            expire_time=expire,
            prefix_messages=prefix_messages,
        )


def _common_kwargs(
    base_url: str,
    timeout: float,
    transport: Optional[httpx.BaseTransport],
    transport_client,
) -> dict:
    out = {"base_url": base_url.rstrip("/"), "timeout": timeout}
    if transport_client is not None:
        # transport_client 由调用方自己放进 kwargs
        pass
    if transport is not None:
        out["transport"] = transport
    return out


# ---------------------------------------------------------------------------
# 同步客户端
# ---------------------------------------------------------------------------


class CachedClient:
    """带 KV Cache 协商的 OpenAI 兼容同步客户端。

    Usage::

        with CachedClient(base_url="http://gateway/v1", api_key="sk-...") as client:
            resp = client.chat_completions(
                model="deepseek-chat",
                messages=[{"role": "user", "content": "Hello"}],
            )

    线程安全。注意:同步客户端无法直接以 ``ASGITransport`` 连接网关
    (ASGITransport 仅支持异步);测试或进程内直连请用 ``AsyncCachedClient``。
    """

    def __init__(
        self,
        base_url: str,
        api_key: str = "unused",
        *,
        tokenizer: Optional[Tokenizer] = None,
        timeout: float = 60.0,
        transport: Optional[httpx.BaseTransport] = None,
        auto_retry_on_miss: bool = True,
        transport_client: Optional[httpx.Client] = None,
    ):
        self.base_url = base_url
        self.api_key = api_key
        self.tokenizer = tokenizer or Tokenizer()
        self.auto_retry_on_miss = auto_retry_on_miss
        if transport_client is not None:
            self._http = transport_client
            self._owns_http = False
        else:
            kw = _common_kwargs(base_url, timeout, transport, transport_client)
            self._http = httpx.Client(**kw)
            self._owns_http = True
        self._cm = _CacheManager()

    def chat_completions(self, *, model: str, messages: List[dict], **extra) -> httpx.Response:
        key = (model, self.base_url)
        state, valid = self._cm.decide_sync(key, messages)

        # sent = 本次实际发送的内容(命中时是增量,全量时是全量)。
        # 更新本地前缀时:命中→旧前缀+增量,全量→全量,二者都正确还原出新前缀。
        sent: List[dict]
        if valid and state is not None:
            prefix_len = len(state.prefix_messages)
            increment = messages[prefix_len:]
            resp = self._send(
                model=model, messages=increment, extra=extra,
                cache_hash=state.cache_hash, prefix_length=prefix_len,
            )
            if (
                self.auto_retry_on_miss
                and resp.status_code == 422
                and not protocol.parse_cache_hit(
                    resp.headers.get(protocol.HEADER_RESP_CACHE_HIT)
                )
            ):
                self._cm.invalidate_sync(key)
                resp = self._send(model=model, messages=messages, extra=extra)
                sent = messages  # 重试发的是全量
            else:
                sent = increment
        else:
            resp = self._send(model=model, messages=messages, extra=extra)
            sent = messages

        self._cm.update_sync(key, sent, resp.headers)
        return resp

    def _send(self, *, model, messages, extra, cache_hash="", prefix_length=None) -> httpx.Response:
        body, headers = _build_send_args(
            model=model, messages=messages, extra=extra,
            cache_hash=cache_hash, prefix_length=prefix_length, api_key=self.api_key,
        )
        return self._http.post("/v1/chat/completions", json=body, headers=headers)

    def close(self) -> None:
        if self._owns_http:
            self._http.close()

    def __enter__(self) -> "CachedClient":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    # 仅供测试/观测
    def _get_cache_state(self, key) -> Optional[_CacheState]:
        return self._cm.get_state_sync(key)

    def _compute_local_hash(self, model: str, messages: List[dict]) -> str:
        return compute_prefix_hash(self.tokenizer.encode_messages(messages))


# ---------------------------------------------------------------------------
# 异步客户端(支持 ASGITransport,用于 async 应用 / 测试 / 进程内直连)
# ---------------------------------------------------------------------------


class AsyncCachedClient:
    """带 KV Cache 协商的 OpenAI 兼容异步客户端。

    与 ``CachedClient`` 语义完全一致,但基于 ``httpx.AsyncClient``,可直接用
    ``httpx.ASGITransport`` 连接网关 ASGI app,无需真实端口。

    Usage::

        async with AsyncCachedClient(base_url="http://gateway/v1") as client:
            resp = await client.chat_completions(model="deepseek-chat", messages=[...])
    """

    def __init__(
        self,
        base_url: str,
        api_key: str = "unused",
        *,
        tokenizer: Optional[Tokenizer] = None,
        timeout: float = 60.0,
        transport: Optional[httpx.BaseTransport] = None,
        auto_retry_on_miss: bool = True,
        transport_client: Optional[httpx.AsyncClient] = None,
    ):
        self.base_url = base_url
        self.api_key = api_key
        self.tokenizer = tokenizer or Tokenizer()
        self.auto_retry_on_miss = auto_retry_on_miss
        if transport_client is not None:
            self._http = transport_client
            self._owns_http = False
        else:
            kw = _common_kwargs(base_url, timeout, transport, transport_client)
            self._http = httpx.AsyncClient(**kw)
            self._owns_http = True
        self._cm = _CacheManager()

    async def chat_completions(self, *, model: str, messages: List[dict], **extra) -> httpx.Response:
        key = (model, self.base_url)
        state, valid = await self._cm.decide_async(key, messages)

        sent: List[dict]
        if valid and state is not None:
            prefix_len = len(state.prefix_messages)
            increment = messages[prefix_len:]
            resp = await self._send(
                model=model, messages=increment, extra=extra,
                cache_hash=state.cache_hash, prefix_length=prefix_len,
            )
            if (
                self.auto_retry_on_miss
                and resp.status_code == 422
                and not protocol.parse_cache_hit(
                    resp.headers.get(protocol.HEADER_RESP_CACHE_HIT)
                )
            ):
                await self._cm.invalidate_async(key)
                resp = await self._send(model=model, messages=messages, extra=extra)
                sent = messages
            else:
                sent = increment
        else:
            resp = await self._send(model=model, messages=messages, extra=extra)
            sent = messages

        await self._cm.update_async(key, sent, resp.headers)
        return resp

    async def _send(self, *, model, messages, extra, cache_hash="", prefix_length=None) -> httpx.Response:
        body, headers = _build_send_args(
            model=model, messages=messages, extra=extra,
            cache_hash=cache_hash, prefix_length=prefix_length, api_key=self.api_key,
        )
        return await self._http.post("/v1/chat/completions", json=body, headers=headers)

    async def close(self) -> None:
        if self._owns_http:
            await self._http.aclose()

    async def __aenter__(self) -> "AsyncCachedClient":
        return self

    async def __aexit__(self, *exc) -> None:
        await self.close()

    # 仅供测试/观测
    async def _get_cache_state(self, key) -> Optional[_CacheState]:
        return await self._cm.get_state_async(key)

    def _compute_local_hash(self, model: str, messages: List[dict]) -> str:
        return compute_prefix_hash(self.tokenizer.encode_messages(messages))
