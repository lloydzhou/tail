"""测试辅助:进程内 ASGI 转发 + 常用 fixture 工厂。

目的:让网关 ↔ 后端、SDK ↔ 网关的请求都走 ``httpx.ASGITransport`` 完成,
**无需监听任何真实 TCP 端口**,测试快、稳定、可并行。

技术说明:``httpx.ASGITransport`` 仅支持异步(只有 ``aclose``/``__aenter__``),
因此同步 ``httpx.Client`` 无法直接包装它。本套测试统一使用 ``AsyncCachedClient``
+ ``httpx.AsyncClient``,以 async 测试覆盖完整链路。
"""

from __future__ import annotations

import contextlib

import httpx
from fastapi import FastAPI

from tail.backend import build_backend_app
from tail.gateway import build_app
from tail.store import PrefixCacheStore
from tail.tokenizer import Tokenizer

MODEL = "deepseek-chat"


@contextlib.asynccontextmanager
async def async_client(app: FastAPI, base_url: str = "http://t.test"):
    """创建一个走 ASGI transport 的异步 httpx.AsyncClient。"""
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url=base_url) as c:
        yield c


def make_backend_transport() -> httpx.ASGITransport:
    return httpx.ASGITransport(app=build_backend_app())


def make_gateway(
    *,
    backend_transport: httpx.BaseTransport | None = None,
    store: PrefixCacheStore | None = None,
    miss_mode: str = "fast_fail",
    cache_ttl: int = 3600,
    ttl_jitter: int = 0,
) -> FastAPI:
    return build_app(
        store=store or PrefixCacheStore(max_entries=100),
        tokenizer=Tokenizer(),
        backend_url="http://backend.test",
        backend_transport=backend_transport or make_backend_transport(),
        miss_mode=miss_mode,
        cache_ttl=cache_ttl,
        ttl_jitter=ttl_jitter,
    )


def make_gateway_transport(
    *,
    backend_transport: httpx.BaseTransport | None = None,
    miss_mode: str = "fast_fail",
    cache_ttl: int = 3600,
) -> tuple[httpx.ASGITransport, FastAPI]:
    app = make_gateway(
        backend_transport=backend_transport,
        miss_mode=miss_mode,
        cache_ttl=cache_ttl,
    )
    return httpx.ASGITransport(app=app), app


def make_async_sdk(
    gateway_app: FastAPI,
    *,
    auto_retry_on_miss: bool = True,
) -> "AsyncCachedClient":  # noqa: F821
    from tail.sdk import AsyncCachedClient

    transport = httpx.ASGITransport(app=gateway_app)
    return AsyncCachedClient(
        base_url="http://gateway.test",
        api_key="sk-test",
        tokenizer=Tokenizer(),
        transport=transport,
        auto_retry_on_miss=auto_retry_on_miss,
    )


async def reset_backend(backend_app: FastAPI) -> None:
    async with async_client(backend_app) as c:
        await c.post("/__backend/reset")


async def get_received(backend_app: FastAPI) -> list[dict]:
    async with async_client(backend_app) as c:
        r = await c.get("/__backend/received")
        return r.json()["requests"]


def make_stack(*, miss_mode: str = "fast_fail", cache_ttl: int = 3600, ttl_jitter: int = 0, auto_retry: bool = True):
    """构造 (backend_app, gateway_app, async_sdk),三方共享 backend transport。"""
    backend_app = build_backend_app()
    gw = build_app(
        store=PrefixCacheStore(max_entries=100),
        tokenizer=Tokenizer(),
        backend_url="http://backend.test",
        backend_transport=httpx.ASGITransport(app=backend_app),
        miss_mode=miss_mode,
        cache_ttl=cache_ttl,
        ttl_jitter=ttl_jitter,
    )
    sdk = make_async_sdk(gw, auto_retry_on_miss=auto_retry)
    return backend_app, gw, sdk
