"""模拟推理服务(Backend)—— 对应设计文档架构图中的「推理服务(DeepSeek 等)」。

接收**标准** OpenAI Chat Completions 请求,无任何缓存感知,返回标准响应。
仅用于开发、联调与测试;生产中替换为真实推理服务即可。

该 app 会把每次收到的 messages 记录到 ``app.state.received``,便于测试断言
「网关到底把什么转发给了后端」——这是验证拼装正确性的关键观测点。
"""

from __future__ import annotations

import time
import uuid

from fastapi import FastAPI, Request


def build_backend_app() -> FastAPI:
    app = FastAPI(title="Mock Inference Backend")
    received: list[dict] = []

    @app.post("/v1/chat/completions")
    async def completions(request: Request):
        try:
            body = await request.json()
        except Exception:
            body = {}
        messages = body.get("messages", []) or []
        model = body.get("model", "deepseek-chat")

        # 记录网关实际转发过来的请求,供测试断言。
        received.append({"messages": messages, "model": model, "stream": body.get("stream", False)})

        last_user_content = ""
        for m in reversed(messages):
            if isinstance(m, dict) and m.get("role") == "user":
                last_user_content = str(m.get("content", ""))
                break

        reply = (
            f"[echo] last_user={last_user_content!r} | "
            f"turn={len(received)} | msgs_seen_by_backend={len(messages)}"
        )

        # 粗略的 prompt token 计数(仅演示,非精确分词)。
        prompt_chars = sum(len(str(m.get("content", ""))) for m in messages if isinstance(m, dict))
        prompt_tokens = max(1, prompt_chars // 4)

        return {
            "id": f"chatcmpl-{uuid.uuid4().hex[:12]}",
            "object": "chat.completion",
            "created": int(time.time()),
            "model": model,
            "choices": [
                {
                    "index": 0,
                    "message": {"role": "assistant", "content": reply},
                    "finish_reason": "stop",
                }
            ],
            "usage": {
                "prompt_tokens": prompt_tokens,
                "completion_tokens": 1,
                "total_tokens": prompt_tokens + 1,
                # 后端 KV Cache 命中字段(与网关缓存无关,按第 4.2 节原样保留)。
                "prompt_cache_hit_tokens": 0,
            },
        }

    @app.get("/__backend/received")
    async def get_received():
        return {"count": len(received), "requests": received}

    @app.post("/__backend/reset")
    async def reset():
        received.clear()
        return {"ok": True}

    app.state.received = received
    return app


def app_factory() -> FastAPI:
    """uvicorn --factory 入口(run.sh 用)。

    用法: ``uvicorn tests.mock_backend:app_factory --factory --port 8080``
    """
    return build_backend_app()
