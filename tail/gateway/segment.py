"""Segment 切分(移植自 Lua segment.lua,设计文档 §3.1)。

Segment = (1×assistant)? + (m×tool) + (n×user)
约束:m + n ≥ 1 且 m·n = 0(tool 与 user 互斥)。
"""

from __future__ import annotations

from typing import List


def _is_assistant(msg: dict) -> bool:
    return msg.get("role") == "assistant"


def _is_tool(msg: dict) -> bool:
    return msg.get("role") == "tool"


def _is_user_like(msg: dict) -> bool:
    return msg.get("role") in ("user", "system")


def split(messages: List[dict]) -> List[List[dict]]:
    """把 messages 切成 segment 列表。不修改输入;展平后与原数组一致。

    状态机:cur_kind 记录当前段非 assistant 消息的类型(None/"tool"/"user")。
    - assistant 遇已闭合段(cur_kind 非 None)→ 开新段
    - tool 遇 user 段 / user 遇 tool 段 → 闭合开新段(m·n=0)
    - 末尾全是 assistant 的段 → 合并回前段(streaming 残留场景)
    """
    if not messages:
        return []

    segments: List[List[dict]] = []
    cur: List[dict] = []
    cur_kind = None  # None / "tool" / "user"

    def close():
        nonlocal cur, cur_kind
        if cur:
            segments.append(cur)
        cur = []
        cur_kind = None

    for msg in messages:
        if _is_assistant(msg):
            if cur_kind is not None:
                close()
            cur.append(msg)
            # assistant 不改变 cur_kind
        elif _is_tool(msg):
            if cur_kind == "user":
                close()
            cur.append(msg)
            cur_kind = "tool"
        else:  # user / system
            if cur_kind == "tool":
                close()
            cur.append(msg)
            cur_kind = "user"
    close()

    # 末尾全是 assistant 的段合并回前段
    if len(segments) >= 2:
        last = segments[-1]
        if not any(not _is_assistant(m) for m in last):
            segments[-2].extend(last)
            segments.pop()
    return segments


def validate(seg: List[dict]) -> bool:
    """校验单个 segment 满足约束(测试用)。"""
    if not seg:
        return False
    n_tool = sum(1 for m in seg if _is_tool(m))
    n_user = sum(1 for m in seg if _is_user_like(m))
    if n_tool + n_user < 1:
        return False
    if n_tool > 0 and n_user > 0:
        return False
    return True


def flatten_match(segments: List[List[dict]], messages: List[dict]) -> bool:
    """展平后与原 messages 完全一致(顺序、引用)。"""
    flat = [m for seg in segments for m in seg]
    return flat == messages
