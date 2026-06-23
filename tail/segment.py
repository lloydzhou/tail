"""Segment 切分 —— 按一个 LLM 回合切分 messages。

对应设计文档 §3.1。

Segment 定义:
    segment = (1 × assistant)? + (m × tool) + (n × user)
    约束:
      - assistant 至多 1 条(回合的回复;首个 segment 可没有)
      - m + n ≥ 1  且  m · n = 0
        (至少一条非 assistant;tool 与 user 不能在同一回合同时出现)

切分算法(顺序扫描):
    遇到 assistant:
      - 若当前 segment 已有非 assistant 消息 → 新 segment 开始,assistant 归新段
      - 否则 assistant 归当前段(回合的回复)
    遇到 tool / user:
      - 归当前段的非 assistant 部分
      - 一旦当前段已有非 assistant 消息,且新消息 role 与已有非 assistant 不同
        → 违反 m·n=0,触发新 segment(m·n=0 约束的体现)

这样得到的 segment 天然对齐 LLM 一来一回的语义边界。
"""

from __future__ import annotations

from typing import List


def _is_assistant(msg: dict) -> bool:
    return msg.get("role") == "assistant"


def _is_tool(msg: dict) -> bool:
    return msg.get("role") == "tool"


def _is_user_like(msg: dict) -> bool:
    """user / system(对话开头常见)归为"非 assistant 非 tool"类。"""
    return msg.get("role") in ("user", "system")


def split_segments(messages: List[dict]) -> List[List[dict]]:
    """把 messages 数组切成 segment 列表。

    不修改输入数组。返回的 segment 拼接后与原数组一致(字节级保持顺序)。

    算法(从语义出发):
      一个 segment = (可选的 1 条 assistant 作为开头) + (m 条 tool 或 n 条 user)。
      assistant 是"回合的回复",它依附于**其后**的非 assistant 消息。
      遇到非 assistant(tool/user)且前一段已闭合 → 开新段,把累积的 assistant 作为新段开头。
      m·n=0:同一段内 tool 与 user 互斥 —— 段内已有 tool 时遇到 user(或反之)→ 闭合,新段。

    >>> split_segments([u("q1"), a("a1"), u("q2")])
    [[{user:q1}], [{assistant:a1}, {user:q2}]]
    """
    if not messages:
        return []

    segments: List[List[dict]] = []
    cur: List[dict] = []
    cur_non_assistant_kind = None  # None / "tool" / "user"

    def close():
        nonlocal cur, cur_non_assistant_kind
        if cur:
            segments.append(cur)
        cur = []
        cur_non_assistant_kind = None

    for msg in messages:
        if _is_assistant(msg):
            # assistant:若当前段已有非 assistant(已闭合形态)→ 开新段
            if cur_non_assistant_kind is not None:
                close()
            cur.append(msg)
            # assistant 不改变 cur_non_assistant_kind(它等"后续"非 assistant)
        elif _is_tool(msg):
            if cur_non_assistant_kind == "user":
                # 段内已有 user,违反 m·n=0 → 闭合开新段
                close()
                # 注意:闭合后,当前 msg(tool)是新段的开始,但它前面没有 assistant
                # 这是合法的(首 segment 可无 assistant,或被前一段的 assistant 错过)
            cur.append(msg)
            cur_non_assistant_kind = "tool"
        else:  # user / system
            if cur_non_assistant_kind == "tool":
                close()
            cur.append(msg)
            cur_non_assistant_kind = "user"
    close()

    # 收尾修正:若最后一段是"孤立的 assistant"(无非 assistant 消息,违反约束),
    # 把它合并回前一段(它是上一回合的延伸,或是被中断的未完成回合)。
    # 这是真实 LLM 流的合法场景(streaming 中断、末尾 assistant 残留)。
    if len(segments) >= 2 and cur_non_assistant_kind is None and cur:
        # cur 此时为空(close 已清),检查 segments 末尾
        pass
    # 重新检查:close 后 cur 已空,看 segments 最后一段
    if segments:
        last = segments[-1]
        last_has_non_assistant = any(not _is_assistant(m) for m in last)
        if not last_has_non_assistant and len(segments) >= 2:
            # 末尾全是 assistant,合并到前一段
            segments[-2].extend(segments[-1])
            segments.pop()
    return segments


def validate_segment(seg: List[dict]) -> bool:
    """校验单个 segment 是否满足定义约束。

    - 至少一条消息
    - 至少一条非 assistant 消息(m+n ≥ 1),除非是末尾合并的孤立 assistant 段(allow_orphan)
    - m(tool 数)· n(user/system 数)= 0(tool 与 user 不同时出现)
    - assistant 可连续(streaming 中断等场景合法)
    """
    if not seg:
        return False
    n_tool = sum(1 for m in seg if _is_tool(m))
    n_user = sum(1 for m in seg if _is_user_like(m))
    if n_tool + n_user < 1:
        return False
    if n_tool > 0 and n_user > 0:
        return False  # m · n != 0
    return True


def segments_match_original(segments: List[List[dict]], messages: List[dict]) -> bool:
    """校验:segments 展平后与原 messages 完全一致(字节级,顺序不变)。"""
    flat = [m for seg in segments for m in seg]
    return flat == messages
