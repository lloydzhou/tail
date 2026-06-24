"""交互式 chat 命令行 —— 用 Tail 网关做一轮真实对话(含工具调用循环)。

用法:
    python -m tail.chat --base-url http://127.0.0.1:8765/v1 \\
        --api-key sk-xxx --model deepseek-chat

启动后直接在命令行输入消息,回车发送。助手可调用 get_current_time 工具,
工具结果会自动喂回,形成多轮工具调用循环。输入 /quit 或 Ctrl+C 退出。

每轮请求都经过 Tail 网关,自动启用前缀缓存协商(从第 2 轮起只发增量)。
配合网关的 --debug 可以在网关侧看到缓存命中日志。
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime

# openai SDK(需已 openai_patch.install 才能启用缓存协商;
# 这里也允许不装 patch 直接用,只是不会自动协商)
from tail import openai_patch


# ---------------------------------------------------------------------------
# 内置工具:get_current_time(返回当前时间戳)
# ---------------------------------------------------------------------------
TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "get_current_time",
            "description": "获取当前的日期和时间(本地时区)。当用户问'现在几点''今天日期'时调用。",
            "parameters": {
                "type": "object",
                "properties": {
                    "timezone": {
                        "type": "string",
                        "description": "可选时区,如 Asia/Shanghai。默认用系统本地时区。",
                    }
                },
            },
        },
    }
]


def execute_tool(name: str, arguments: dict) -> str:
    """执行工具调用,返回结果字符串。"""
    if name == "get_current_time":
        now = datetime.now()
        return now.strftime("%Y-%m-%d %H:%M:%S (%A)")
    return f"未知工具: {name}"


# ---------------------------------------------------------------------------
# 工具调用循环
# ---------------------------------------------------------------------------


def chat_with_tool_loop(client, model: str, messages: list, max_rounds: int = 5) -> str:
    """发起一次 chat,如果模型要调工具,执行后把结果喂回,循环直到拿到最终文本。

    返回最终的 assistant 文本。messages 会被原地修改(追加 assistant/tool 消息)。
    """
    for _ in range(max_rounds):
        resp = client.chat.completions.create(
            model=model,
            messages=messages,
            tools=TOOLS,
        )
        msg = resp.choices[0].message
        # 把 assistant 回复(含 tool_calls)加入历史
        messages.append(msg.model_dump(exclude_none=True))

        # 没有工具调用 → 拿到最终文本,结束循环
        if not msg.tool_calls:
            return msg.content or ""

        # 有工具调用 → 逐个执行,把结果作为 tool 消息喂回
        for tc in msg.tool_calls:
            fn_name = tc.function.name
            try:
                fn_args = json.loads(tc.function.arguments or "{}")
            except json.JSONDecodeError:
                fn_args = {}
            print(f"  🔧 调用工具 {fn_name}({fn_args})")
            result = execute_tool(fn_name, fn_args)
            print(f"  ← {result}")
            messages.append({
                "role": "tool",
                "tool_call_id": tc.id,
                "content": result,
            })
        # 继续下一轮,让模型基于工具结果继续

    return "(达到最大工具调用轮数)"


# ---------------------------------------------------------------------------
# 主循环
# ---------------------------------------------------------------------------


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(
        prog="python -m tail.chat",
        description="Tail 交互式 chat —— 经 Tail 网关做真实对话(含工具调用循环)",
    )
    parser.add_argument("--base-url", default="http://127.0.0.1:8765/v1",
                        help="Tail 网关地址(默认 http://127.0.0.1:8765/v1)")
    parser.add_argument("--api-key", default="sk-tail-demo",
                        help="API key(透传给后端;真实后端需有效 key)")
    parser.add_argument("--model", default="deepseek-chat",
                        help="模型名(默认 deepseek-chat)")
    parser.add_argument("--system", default="你是 Tail 的演示助手。可以调用 get_current_time 查询当前时间。回答简洁。",
                        help="system prompt")
    parser.add_argument("--no-patch", action="store_true",
                        help="不装 openai_patch(则不会自动协商前缀缓存,纯透传)")
    args = parser.parse_args(argv)

    # 装 patch(启用前缀缓存协商)
    if not args.no_patch:
        openai_patch.install()
        print(f"✓ 已启用 openai_patch(前缀缓存协商:从第 2 轮起只发增量)")
    else:
        print("(未装 patch,纯透传模式)")

    from openai import OpenAI
    client = OpenAI(base_url=args.base_url, api_key=args.api_key)

    messages = [{"role": "system", "content": args.system}]

    print(f"\nTail Chat  (model={args.model}, gateway={args.base_url})")
    print(f"内置工具: get_current_time | 命令: /quit 退出 /clear 清空历史\n")

    while True:
        try:
            user_input = input("你> ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\n再见!")
            break
        if not user_input:
            continue
        if user_input in ("/quit", "/exit", "/q"):
            print("再见!")
            break
        if user_input == "/clear":
            messages = [{"role": "system", "content": args.system}]
            print("(历史已清空)\n")
            continue

        messages.append({"role": "user", "content": user_input})
        try:
            reply = chat_with_tool_loop(client, args.model, messages)
            print(f"🤖 {reply}\n")
        except Exception as e:
            print(f"⚠️ 出错: {e}\n")
            # 出错时移除刚加的 user 消息,避免历史污染
            if messages and messages[-1].get("role") == "user":
                messages.pop()


if __name__ == "__main__":
    main()
