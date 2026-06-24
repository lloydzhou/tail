"""交互式 chat 命令行 —— 用 Tail 网关做真实对话(含工具调用循环 + compact 压缩)。

用法:
    python -m tail.chat --base-url http://127.0.0.1:8765/v1 \\
        --api-key sk-xxx --model deepseek-chat

启动后直接在命令行输入消息,回车发送。助手可调用 get_current_time 工具,
工具结果会自动喂回,形成多轮工具调用循环。输入 /quit 或 Ctrl+C 退出。

每轮请求都经过 Tail 网关,自动启用前缀缓存协商(从第 2 轮起只发增量)。
配合网关的 --debug 可以在网关侧看到缓存命中日志。

compact 机制:每 10 个 user prompt 后自动压缩,只保留 system + 最近 N 个
user prompt 的完整回合(默认 N=3)。compact 后 SDK 会发现前缀指纹不匹配,
自动降级发全量重建缓存(这正是 Tail 的 SDK 一致性保障在起作用)。
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime

from tail import openai_patch


# ---------------------------------------------------------------------------
# 配置
# ---------------------------------------------------------------------------
COMPACT_EVERY = 10       # 每 10 个 user prompt 触发 compact
COMPACT_KEEP = 3         # compact 后保留最近 3 个 user prompt 的回合


# ---------------------------------------------------------------------------
# 内置工具:get_current_time
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
    if name == "get_current_time":
        now = datetime.now()
        return now.strftime("%Y-%m-%d %H:%M:%S (%A)")
    return f"未知工具: {name}"


# ---------------------------------------------------------------------------
# user prompt 计数(排除 tool_result)
# ---------------------------------------------------------------------------

def _is_real_user_prompt(msg: dict) -> bool:
    """判断是否为"真正的用户输入",而非 tool_result。

    OpenAI 格式:tool_result 的 role 是 "tool"(带 tool_call_id)。
    但有些 API/适配层把 tool_result 也标成 role="user"(带 tool_call_id)。
    这里两种都排除:只要有 tool_call_id,就不是真正的用户输入。
    """
    if msg.get("role") != "user":
        return False
    # 有 tool_call_id 的是 tool_result 伪装的 user,排除
    if msg.get("tool_call_id"):
        return False
    # content 为 None/空且带 name 的也可能是 tool 相关,排除
    if not msg.get("content"):
        return False
    return True


def count_user_prompts(messages: list) -> int:
    """统计真正的 user prompt 数量(不含 tool_result)。"""
    return sum(1 for m in messages if _is_real_user_prompt(m))


# ---------------------------------------------------------------------------
# compact:保留 system + 最近 keep 个 user prompt 的完整回合
# ---------------------------------------------------------------------------

def compact_messages(messages: list, keep: int = COMPACT_KEEP) -> list:
    """压缩 messages:保留 system 前导 + 最近 keep 个 user prompt 起的完整回合。

    关键约束:不从 tool_result 中间切。切点必须是某个 user prompt 的起始位置,
    该 user prompt 之后(含它对应的 assistant 回复、tool 调用、tool 结果)全部保留。

    Args:
      messages: 当前完整消息列表
      keep:     保留最近几个 user prompt 的回合(默认 3)
    Returns:
      压缩后的 messages(新建列表,不修改原列表)
    """
    if not messages:
        return []

    # 1. 分离 system 前导(开头的连续 system 消息)
    sys_msgs = []
    i = 0
    while i < len(messages) and messages[i].get("role") == "system":
        sys_msgs.append(messages[i])
        i += 1
    rest = messages[i:]  # 非前导部分(对话)

    if not rest:
        return list(sys_msgs)

    # 2. 从末尾往前找 keep 个真正的 user prompt 的 index
    user_indices = []
    for idx in range(len(rest) - 1, -1, -1):
        if _is_real_user_prompt(rest[idx]):
            user_indices.append(idx)
            if len(user_indices) >= keep:
                break

    if len(user_indices) < keep:
        # user prompt 不足 keep 个,不压缩
        return list(messages)

    # 3. 切点 = 第 keep 个 user prompt 的 index(从末尾数)
    #    user_indices 是从后往前的,最后一个元素是第 keep 个(最靠前的)
    cut_index = user_indices[-1]

    # 4. 验证切点安全:切点必须是 user prompt(role=user 无 tool_call_id)
    #    这样保证不会从 tool_result 或 assistant 中间切
    #    (如果第 keep 个 user prompt 前面是 tool_result,那 tool_result 属于上一回合,
    #     被切掉是对的;切点本身是 user,其后的 assistant/tool 都完整保留)
    cut_msg = rest[cut_index]
    assert _is_real_user_prompt(cut_msg), f"切点不是 user prompt: {cut_msg}"

    # 5. 构造压缩后的 messages = system 前导 + rest[cut_index:]
    compacted = list(sys_msgs) + rest[cut_index:]
    return compacted


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
        messages.append(msg.model_dump(exclude_none=True))

        if not msg.tool_calls:
            return msg.content or ""

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

    return "(达到最大工具调用轮数)"


# ---------------------------------------------------------------------------
# 主循环
# ---------------------------------------------------------------------------

def main(argv=None) -> None:
    parser = argparse.ArgumentParser(
        prog="python -m tail.chat",
        description="Tail 交互式 chat —— 经 Tail 网关做真实对话(含工具调用循环 + compact)",
    )
    parser.add_argument("--base-url", default="http://127.0.0.1:8765/v1",
                        help="Tail 网关地址(默认 http://127.0.0.1:8765/v1)")
    parser.add_argument("--api-key", default="sk-tail-demo",
                        help="API key(透传给后端;真实后端需有效 key)")
    parser.add_argument("--model", default="deepseek-chat",
                        help="模型名(默认 deepseek-chat)")
    parser.add_argument("--system", default="你是 Tail 的演示助手。可以调用 get_current_time 查询当前时间。回答简洁。",
                        help="system prompt")
    parser.add_argument("--compact-every", type=int, default=COMPACT_EVERY,
                        help=f"每 N 个 user prompt 触发 compact(默认 {COMPACT_EVERY})")
    parser.add_argument("--compact-keep", type=int, default=COMPACT_KEEP,
                        help=f"compact 后保留最近 N 个 user prompt 回合(默认 {COMPACT_KEEP})")
    parser.add_argument("--no-patch", action="store_true",
                        help="不装 openai_patch(纯透传模式)")
    parser.add_argument("--no-compact", action="store_true",
                        help="禁用 compact(对照测试用)")
    args = parser.parse_args(argv)

    if not args.no_patch:
        openai_patch.install()
        print(f"✓ 已启用 openai_patch(前缀缓存协商:从第 2 轮起只发增量)")
    else:
        print("(未装 patch,纯透传模式)")

    from openai import OpenAI
    client = OpenAI(base_url=args.base_url, api_key=args.api_key)

    system_msg = {"role": "system", "content": args.system}
    messages = [system_msg]

    print(f"\nTail Chat  (model={args.model}, gateway={args.base_url})")
    print(f"工具: get_current_time | compact: 每{args.compact_every}个user prompt压缩至最近{args.compact_keep}个")
    print(f"命令: /quit 退出 /clear 清空 /compact 手动压缩\n")

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
            messages = [system_msg]
            print("(历史已清空)\n")
            continue
        if user_input == "/compact":
            before = count_user_prompts(messages)
            messages = compact_messages(messages, args.compact_keep)
            after = count_user_prompts(messages)
            print(f"(手动 compact: {before} → {after} 个 user prompt,{len(messages)} 条消息)\n")
            continue

        messages.append({"role": "user", "content": user_input})

        # 自动 compact 检查
        if not args.no_compact:
            n_user = count_user_prompts(messages)
            if n_user >= args.compact_every:
                before = len(messages)
                messages = compact_messages(messages, args.compact_keep)
                print(f"  📦 [compact] {n_user} 个 user prompt 触发压缩 → 保留最近 {args.compact_keep} 个 "
                      f"({before} → {len(messages)} 条消息;下次请求将发全量重建缓存)")

        try:
            reply = chat_with_tool_loop(client, args.model, messages)
            print(f"🤖 {reply}\n")
        except Exception as e:
            print(f"⚠️ 出错: {e}\n")
            if messages and messages[-1].get("role") == "user":
                messages.pop()


if __name__ == "__main__":
    main()
