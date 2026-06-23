"""端到端演示:Client SDK → 网关 → 模拟后端,完整多轮对话 + 带宽节省统计。

无需任何真实 LLM、无需监听端口,全部走进程内 ASGI。直接运行::

    python examples/demo.py

展示设计文档第 3、5、6 章的核心协议行为:
  - 首次请求全量发送,网关返回缓存哈希;
  - 后续请求 SDK 自动只发增量,网关命中后还原完整请求转发后端;
  - 模拟缓存失效,SDK 自动降级重试;
  - 打印带宽节省统计。
"""

from __future__ import annotations

import asyncio
import json
import os
import sys

# 让 demo 可直接 ``python examples/demo.py`` 运行,无需安装包。
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

import httpx

from tail import protocol
from tail.backend import build_backend_app
from tail.gateway import build_app
from tail.sdk import AsyncCachedClient
from tail.store import PrefixCacheStore
from tail.tokenizer import Tokenizer

MODEL = "deepseek-chat"


def _banner(title: str) -> None:
    print("\n" + "=" * 72)
    print(f"  {title}")
    print("=" * 72)


async def main() -> None:
    # 组装链路:模拟后端(ASGI) ← 网关(ASGI,以后端为上游) ← SDK(ASGI)
    backend = build_backend_app()
    gateway = build_app(
        store=PrefixCacheStore(max_entries=1000),
        tokenizer=Tokenizer(),
        backend_url="http://backend.test",
        backend_transport=httpx.ASGITransport(app=backend),
        miss_mode="fast_fail",
    )

    sdk = AsyncCachedClient(
        base_url="http://gateway.test",
        api_key="sk-demo",
        tokenizer=Tokenizer(),
        transport=httpx.ASGITransport(app=gateway),
    )

    # 一个很长的 system 前缀,放大带宽节省效果。
    long_system = {
        "role": "system",
        "content": (
            "你是一个严谨的中文助手,请基于事实回答。" * 100
        ),
    }

    # ---------- 第 1 轮:首次,全量发送 ----------
    _banner("第 1 轮(首次请求:全量发送)")
    msgs1 = [long_system, {"role": "user", "content": "你好"}]
    r1 = await sdk.chat_completions(model=MODEL, messages=msgs1)
    print(f"  X-Cache-Hit   : {r1.headers[protocol.HEADER_RESP_CACHE_HIT]}")
    print(f"  X-Cache-Hash  : {r1.headers[protocol.HEADER_RESP_CACHE_HASH]}")
    body1 = r1.json()
    print(f"  assistant 回复: {body1['choices'][0]['message']['content']}")

    # ---------- 第 2 轮:增量发送,应命中 ----------
    _banner("第 2 轮(增量发送:应命中缓存)")
    msgs2 = msgs1 + [
        {"role": "assistant", "content": "你好!有什么可以帮你的?"},
        {"role": "user", "content": "今天天气怎么样?"},
    ]
    r2 = await sdk.chat_completions(model=MODEL, messages=msgs2)
    print(f"  X-Cache-Hit   : {r2.headers[protocol.HEADER_RESP_CACHE_HIT]}")
    print(f"  X-Cache-Hash  : {r2.headers[protocol.HEADER_RESP_CACHE_HASH]}")
    print(f"  assistant 回复: {r2.json()['choices'][0]['message']['content']}")

    # ---------- 第 3 轮:继续增量 ----------
    _banner("第 3 轮(增量发送:继续命中)")
    msgs3 = msgs2 + [
        {"role": "assistant", "content": "我无法获取实时天气,请查询气象服务。"},
        {"role": "user", "content": "那讲个笑话吧"},
    ]
    r3 = await sdk.chat_completions(model=MODEL, messages=msgs3)
    print(f"  X-Cache-Hit   : {r3.headers[protocol.HEADER_RESP_CACHE_HIT]}")
    print(f"  assistant 回复: {r3.json()['choices'][0]['message']['content']}")

    # ---------- 模拟缓存失效:清空网关缓存,SDK 自动降级重试 ----------
    _banner("模拟缓存失效(清空网关缓存后,SDK 自动重试一次)")
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=gateway), base_url="http://g") as c:
        await c.post("/__gateway/clear-cache")
    msgs4 = msgs3 + [
        {"role": "assistant", "content": "好的。"},
        {"role": "user", "content": "继续聊"},
    ]
    r4 = await sdk.chat_completions(model=MODEL, messages=msgs4)
    print(f"  最终 X-Cache-Hit: {r4.headers[protocol.HEADER_RESP_CACHE_HIT]}")
    print(f"  最终 HTTP 状态  : {r4.status_code}  (SDK 已透明重试)")

    # ---------- 统计 ----------
    _banner("网关统计")
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=gateway), base_url="http://g") as c:
        stats = (await c.get("/__gateway/stats")).json()
    print(json.dumps(stats, indent=2, ensure_ascii=False))
    client_bytes = stats["bytes_from_client"]
    backend_bytes = stats["bytes_to_backend"]
    saved = backend_bytes - client_bytes
    ratio = saved / backend_bytes * 100 if backend_bytes else 0
    print(f"\n  客户端共上传 : {client_bytes:>10,} bytes")
    print(f"  网关转后端   : {backend_bytes:>10,} bytes")
    print(f"  节省上传量   : {saved:>10,} bytes  ({ratio:.1f}%)")

    await sdk.close()


if __name__ == "__main__":
    asyncio.run(main())
