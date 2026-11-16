"""握手:客户端怎么知道对端接不接得住尾巴。

尾巴不是 JSON,所以发出去之前得先问一句 ``OPTIONS``。这里只测这三件事:

  1. 本服务怎么宣告(哪些路径答 OPTIONS、答什么头);
  2. 客户端怎么看那份应答(严到什么程度才算「支持」);
  3. 看错了怎么收场——不知道、不支持、说支持却反悔,三种都得是一轮成功的调用。

tail 协议本身在 ``test_transport.py`` 里测,端到端链路在 ``test_proxy.py`` 里测。
"""

from __future__ import annotations

import asyncio
import time

import httpx2 as httpx
import pytest
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response
from fastapi.testclient import TestClient

from tail import proxy, serve
from tail.transport import gateway as gw
from tail.transport import storage
from tail.transport.chunking import BLOCK_SIZE

from .mock_backend import build_backend_app

CHAT = "/v1/chat/completions"


def _payload(turns: int = 6) -> dict:
    messages = [{"role": "system", "content": "你是助手。" + "规则 " * 200}]
    for i in range(turns):
        messages.append({"role": "user", "content": f"q{i} " + "数据 分析 " * 60})
        messages.append({"role": "assistant", "content": f"a{i} " + "结果 " * 60})
    return {"model": "deepseek-chat", "messages": messages, "stream": False}


# ---------------------------------------------------------------------------
# 几个「对端」:一个只会吃 JSON,一个嘴上说支持,一个答 OPTIONS 很慢
# ---------------------------------------------------------------------------

def _json_only_backend() -> FastAPI:
    """只吃 application/json 的后端:OPTIONS 405,非 JSON 一律 415。"""
    app = FastAPI()

    @app.post(CHAT)
    async def chat(request: Request):
        if "json" not in request.headers.get("content-type", ""):
            return JSONResponse({"error": "unsupported media type"}, status_code=415)
        await request.body()
        return JSONResponse({"object": "chat.completion", "choices": []})

    return app


def _lying_backend() -> FastAPI:
    """OPTIONS 答「支持」,真收到尾巴又 415 —— 中间有东西替它答话时的样子。"""
    app = FastAPI()

    @app.options(CHAT)
    async def handshake():
        return Response(status_code=204, headers=gw.handshake_headers())

    @app.post(CHAT)
    async def chat(request: Request):
        if gw.CONTENT_TYPE_BLOCKS in request.headers.get("content-type", ""):
            return JSONResponse({"error": "unsupported media type"}, status_code=415)
        await request.body()
        return JSONResponse({"object": "chat.completion", "choices": []})

    return app


def _policy_backend() -> FastAPI:
    """接尾巴,但这一轮真的超长了:400 与 json/content-type 无关,不该被当成不支持。"""
    app = FastAPI()

    @app.options(CHAT)
    async def handshake():
        return Response(status_code=204, headers=gw.handshake_headers())

    @app.post(CHAT)
    async def chat(request: Request):
        if gw.CONTENT_TYPE_BLOCKS in request.headers.get("content-type", ""):
            return JSONResponse({"error": {"message": "context length exceeded"}},
                                status_code=400)
        await request.body()
        return JSONResponse({"object": "chat.completion", "choices": []})

    return app


def _slow_options_backend(delay: float = 1.0) -> FastAPI:
    """答 OPTIONS 要 delay 秒:用来证明这一问不在请求路径上。"""
    app = FastAPI()

    @app.options(CHAT)
    async def handshake():
        await asyncio.sleep(delay)
        return Response(status_code=204, headers=gw.handshake_headers())

    @app.post(CHAT)
    async def chat(request: Request):
        await request.body()
        return JSONResponse({"object": "chat.completion", "choices": []})

    return app


def _supporting_gateway() -> FastAPI:
    """真的 tail 网关:答 OPTIONS,吃尾巴(接在 mock 后端上)。"""
    backend = build_backend_app()
    app = serve.build_app("http://mock", storage.MemoryChainStore())
    app.state.client = httpx.AsyncClient(
        transport=httpx.ASGITransport(app=backend), timeout=30)
    return app


def _proxy_over(upstream_app: FastAPI) -> FastAPI:
    app = proxy.build_app("http://upstream")
    app.state.client = httpx.AsyncClient(
        transport=httpx.ASGITransport(app=upstream_app), timeout=30)
    return app


async def _settle(app: FastAPI) -> None:
    """等后台那次握手回来(它不在请求路径上,测试里得自己等)。"""
    pending = [t for t in app.state.probes.values() if not t.done()]
    if pending:
        await asyncio.gather(*pending)


def _fire(app: FastAPI, payload: dict, times: int = 1) -> list:
    """依次发 times 次同一个请求,每次等后台握手落定再发下一次。"""
    out = []

    async def run():
        async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), timeout=30) as client:
            for _ in range(times):
                r = await client.post(CHAT, json=payload)
                await r.aread()
                await r.aclose()
                out.append(r)
                await _settle(app)

    asyncio.run(run())
    return out


# ---------------------------------------------------------------------------
# 1. 服务端怎么宣告
# ---------------------------------------------------------------------------

class TestAdvertisement:
    def test_the_chat_path_advertises_the_wire(self):
        tc = TestClient(_supporting_gateway())
        for path in (CHAT, "/chat/completions"):
            r = tc.options(path)
            assert r.status_code == 204
            assert r.content == b""
            assert r.headers["allow"] == "POST, OPTIONS"
            assert r.headers[gw.HEADER_VERSION] == gw.TAIL_VERSION
            assert r.headers[gw.HEADER_BLOCK] == str(BLOCK_SIZE)

    def test_other_paths_do_not_advertise(self):
        """不吃尾巴的路径不该广告,否则客户端会把尾巴发到错地方。"""
        tc = TestClient(_supporting_gateway())
        r = tc.options("/v1/embeddings")
        assert r.status_code == 405
        assert gw.HEADER_VERSION not in r.headers

    def test_a_client_that_never_asks_still_works(self):
        """老客户端直接发尾巴、也直接发 JSON,两种都照旧。"""
        tc = TestClient(_supporting_gateway())
        r = tc.post(CHAT, json=_payload(2))
        assert r.status_code == 200


# ---------------------------------------------------------------------------
# 2. 怎么读那份应答:只有版本和块大小都对上才算支持
# ---------------------------------------------------------------------------

class TestReadingTheAnswer:
    def test_a_matching_answer_counts(self):
        assert gw.takes_blocks(gw.handshake_headers()) is True

    def test_a_plain_200_counts_as_no(self):
        """代答的代理会 200 但没有这两个头——不能当成支持。"""
        assert gw.takes_blocks({}) is False
        assert gw.takes_blocks({"allow": "POST, OPTIONS"}) is False

    @pytest.mark.parametrize("headers", [
        {gw.HEADER_VERSION: "2", gw.HEADER_BLOCK: "512"},
        {gw.HEADER_VERSION: "3", gw.HEADER_BLOCK: "256"},
        {gw.HEADER_VERSION: "3"},
        {gw.HEADER_BLOCK: "512"},
        {gw.HEADER_VERSION: "", gw.HEADER_BLOCK: ""},
    ])
    def test_anything_else_is_no(self, headers):
        assert gw.takes_blocks(headers) is False

    def test_extra_headers_do_not_matter(self):
        headers = dict(gw.handshake_headers(), **{"x-request-id": "abc"})
        assert gw.takes_blocks(headers) is True


# ---------------------------------------------------------------------------
# 3. 记多久:三种状态、两种 TTL
# ---------------------------------------------------------------------------

class _Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


class TestCapability:
    def test_a_stranger_is_unknown_not_a_yes(self):
        caps = gw.Capability(clock=_Clock())
        assert caps.state("u") == gw.UNKNOWN
        assert caps.takes("u") is False
        assert caps.refuses("u") is False

    def test_a_yes_is_believed_for_a_while(self):
        clock = _Clock()
        caps = gw.Capability(clock=clock)
        caps.set("u", True)
        assert caps.takes("u") is True
        clock.now += gw.TAKES_FOR_SECONDS - 1
        assert caps.takes("u") is True
        clock.now += 2
        assert caps.state("u") == gw.UNKNOWN

    def test_a_no_is_asked_again_sooner(self):
        """对端升级了不该被永远当成旧的,所以「不支持」的 TTL 更短。"""
        clock = _Clock()
        caps = gw.Capability(clock=clock)
        caps.set("u", False)
        assert caps.refuses("u") is True
        assert gw.NO_FOR_SECONDS < gw.TAKES_FOR_SECONDS
        clock.now += gw.NO_FOR_SECONDS + 1
        assert caps.state("u") == gw.UNKNOWN

    def test_answers_are_per_url(self):
        caps = gw.Capability(clock=_Clock())
        caps.set("a", True)
        assert caps.takes("a") is True
        assert caps.takes("b") is False

    def test_snapshot_only_shows_live_answers(self):
        clock = _Clock()
        caps = gw.Capability(clock=clock)
        caps.set("a", True)
        caps.set("b", False)
        assert caps.snapshot() == {"a": gw.TAKES, "b": gw.REFUSES}
        clock.now += gw.TAKES_FOR_SECONDS + 1
        assert caps.snapshot() == {}


# ---------------------------------------------------------------------------
# 4. 客户端:第一轮先整包,问清楚了再发尾巴
# ---------------------------------------------------------------------------

class TestHandshakeOnTheWayOut:
    def test_first_turn_is_plain_then_it_tails(self):
        """不知道就先整包发;后台问到了,下一轮起才换成尾巴。"""
        upstream = _supporting_gateway()
        app = _proxy_over(upstream)
        payload = _payload()

        first, second, third = _fire(app, payload, times=3)

        stats = app.state.stats.as_dict()
        assert app.state.caps.takes("http://upstream" + CHAT) is True
        assert stats["probes"] == 1
        assert stats["plain_requests"] == 1      # 只有第一轮
        assert stats["tail_requests"] == 2
        assert stats["misses"] == 1              # 第二轮:从头开始
        assert stats["hits"] == 1                # 第三轮:只发尾巴
        assert stats["fallbacks"] == 0
        for r in (first, second, third):
            assert r.status_code == 200

        # 三轮总共发上去的字节,比三轮 JSON 少(尾巴换来的就是这点)
        assert stats["wire_bytes_out"] < stats["json_bytes_in"]

    def test_the_probe_never_blocks_a_request(self):
        """对端答 OPTIONS 要 1 秒,而第一轮请求不能跟着等。"""
        app = _proxy_over(_slow_options_backend(1.0))
        payload = _payload(2)

        async def run():
            async with httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=app), timeout=30) as client:
                started = time.monotonic()
                r = await client.post(CHAT, json=payload)
                await r.aread()
                elapsed = time.monotonic() - started
                await _settle(app)
                return r, elapsed

        r, elapsed = asyncio.run(run())
        assert r.status_code == 200
        assert elapsed < 0.5, f"请求等了 {elapsed:.2f}s,握手不该在请求路径上"
        assert app.state.stats.probes == 1

    def test_only_one_probe_per_url(self):
        """并发撞上来时不要每个请求都问一次。"""
        app = _proxy_over(_slow_options_backend(0.3))
        payload = _payload(2)

        async def run():
            async with httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=app), timeout=30) as client:
                await asyncio.gather(*[client.post(CHAT, json=payload)
                                       for _ in range(4)])
                await _settle(app)

        asyncio.run(run())
        assert app.state.stats.probes == 1
        assert app.state.stats.plain_requests == 4

    def test_a_peer_that_says_no_stays_plain(self):
        """答不上 OPTIONS 的对端:一直整包发,一次尾巴都不试。"""
        app = _proxy_over(_json_only_backend())
        payload = _payload()

        for r in _fire(app, payload, times=3):
            assert r.status_code == 200

        stats = app.state.stats.as_dict()
        assert app.state.caps.refuses("http://upstream" + CHAT) is True
        assert stats["plain_requests"] == 3
        assert stats["tail_requests"] == 0
        assert stats["fallbacks"] == 0
        assert stats["probes"] == 1               # 问过一次,记住「不支持」
        assert stats["wire_bytes_out"] == stats["json_bytes_in"]

    def test_a_peer_that_says_yes_then_refuses_falls_back(self):
        """嘴上说支持、真收到尾巴却 415:整包重发一次,这一轮照样成功。"""
        app = _proxy_over(_lying_backend())
        payload = _payload()

        first, second, third = _fire(app, payload, times=3)

        for r in (first, second, third):
            assert r.status_code == 200
            assert r.json()["object"] == "chat.completion"
        stats = app.state.stats.as_dict()
        assert stats["fallbacks"] == 1            # 只有那一轮
        assert app.state.caps.refuses("http://upstream" + CHAT) is True
        assert stats["probes"] == 1               # 反悔之后不再问

    def test_a_real_rejection_is_not_mistaken_for_a_refusal(self):
        """400 说的是「上下文超长」,不是「读不懂你这个 content type」。"""
        app = _proxy_over(_policy_backend())
        payload = _payload(2)

        first, second = _fire(app, payload, times=2)

        assert first.status_code == 200
        assert second.status_code == 400
        assert "context length exceeded" in second.json()["error"]["message"]
        stats = app.state.stats.as_dict()
        assert stats["fallbacks"] == 0            # 没有多打一次
        assert app.state.caps.takes("http://upstream" + CHAT) is True

    def test_the_plain_turn_hides_the_wire_headers(self):
        """第 1 轮整包发,对端应答里的 X-Tail-* 也不能漏给客户端。"""
        app = _proxy_over(_supporting_gateway())
        r, = _fire(app, _payload(2))
        assert r.status_code == 200
        assert not [k for k in r.headers if k.lower().startswith("x-tail-")]

    def test_stats_report_the_handshake(self):
        app = _proxy_over(_supporting_gateway())
        _fire(app, _payload(2))
        stats = TestClient(app).get("/__tail/stats").json()
        assert stats["probes"] == 1
        assert list(stats["capability"].values()) == ["takes"]
