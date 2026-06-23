"""Segment 切分测试 —— 纯逻辑,不依赖 Kvrocks。

验证设计文档 §3.1 的所有合法/非法形态,以及 m·n=0 约束。
"""

from __future__ import annotations

from tail.segment import split_segments, validate_segment, segments_match_original


def u(c): return {"role": "user", "content": c}
def a(c): return {"role": "assistant", "content": c}
def s(c): return {"role": "system", "content": c}
def t(c): return {"role": "tool", "content": c}


# ---------------------------------------------------------------------------
# 基础形态
# ---------------------------------------------------------------------------


def test_first_segment_no_assistant():
    """首 segment 可无 assistant:[system, user]。"""
    segs = split_segments([s("S"), u("q1")])
    assert len(segs) == 1
    assert validate_segment(segs[0])
    assert segs[0] == [s("S"), u("q1")]


def test_standard_qa_rounds():
    """标准一问一答:每回合 [assistant, user]。"""
    segs = split_segments([u("q1"), a("a1"), u("q2"), a("a2"), u("q3")])
    assert len(segs) == 3
    assert segs[0] == [u("q1")]
    assert segs[1] == [a("a1"), u("q2")]
    assert segs[2] == [a("a2"), u("q3")]
    for seg in segs:
        assert validate_segment(seg)


def test_single_tool_call():
    """单工具回合:[assistant, tool](m=1, n=0)。"""
    segs = split_segments([u("调用"), a("call"), t("result"), a("总结"), u("继续")])
    assert len(segs) == 3
    assert segs[0] == [u("调用")]
    assert segs[1] == [a("call"), t("result")]
    assert segs[2] == [a("总结"), u("继续")]
    for seg in segs:
        assert validate_segment(seg)


def test_parallel_tools():
    """并行工具 + 后续 assistant 总结:[u], [a, t, t, a](m=2, n=0)。

    末尾的 assistant 总结合并进工具段(streaming 续写场景)。
    """
    segs = split_segments([u("q"), a("call"), t("r1"), t("r2"), a("总结")])
    assert len(segs) == 2
    assert segs[0] == [u("q")]
    assert segs[1] == [a("call"), t("r1"), t("r2"), a("总结")]
    assert validate_segment(segs[1])  # m=2, n=0, 合法


def test_consecutive_user_messages():
    """续写停止:[assistant, user, user](n=2, m=0)。"""
    segs = split_segments([u("q1"), a("a1"), u("q2"), u("q3")])
    assert len(segs) == 2
    assert segs[1] == [a("a1"), u("q2"), u("q3")]
    assert validate_segment(segs[1])


# ---------------------------------------------------------------------------
# m · n = 0 约束(核心)
# ---------------------------------------------------------------------------


def test_tool_then_user_starts_new_segment():
    """关键:tool 之后遇到 user → 新 segment(m·n=0 强制)。

    [assistant, tool, user] 不会是单 segment,而是 [assistant, tool] + [user]。
    """
    segs = split_segments([a("call"), t("r"), u("new_question")])
    assert len(segs) == 2
    assert segs[0] == [a("call"), t("r")]
    assert segs[1] == [u("new_question")]
    # 每个都满足 m·n=0
    assert validate_segment(segs[0])  # m=1, n=0
    assert validate_segment(segs[1])  # m=0, n=1


def test_user_then_tool_starts_new_segment():
    """user 之后遇到 tool → 新 segment。"""
    segs = split_segments([a("reply"), u("msg"), t("result")])
    assert len(segs) == 2
    assert segs[0] == [a("reply"), u("msg")]
    assert segs[1] == [t("result")]


# ---------------------------------------------------------------------------
# 不变量:展平后字节级一致
# ---------------------------------------------------------------------------


def test_flatten_matches_original_simple():
    msgs = [s("S"), u("q1"), a("a1"), u("q2")]
    segs = split_segments(msgs)
    assert segments_match_original(segs, msgs)


def test_flatten_matches_original_complex():
    msgs = [u("q"), a("c"), t("r1"), t("r2"), a("s"), u("n"), a("c2"), t("r3")]
    segs = split_segments(msgs)
    assert segments_match_original(segs, msgs)


def test_empty_messages():
    assert split_segments([]) == []


def test_single_message():
    segs = split_segments([u("only")])
    assert len(segs) == 1
    assert segs[0] == [u("only")]


# ---------------------------------------------------------------------------
# validate_segment 边界
# ---------------------------------------------------------------------------


def test_validate_allows_consecutive_assistants():
    """连续 assistant(streaming 中断)合法 —— 只要段内有非 assistant。"""
    assert validate_segment([a("1"), a("2"), u("q")])


def test_validate_rejects_all_assistant():
    """全是 assistant(无非 assistant)→ 非法(m+n=0)。"""
    assert not validate_segment([a("1")])


def test_validate_rejects_tool_and_user_mix():
    """m·n != 0 → 非法。"""
    assert not validate_segment([t("r"), u("q")])


def test_validate_accepts_all_legal_forms():
    assert validate_segment([u("q")])               # m=0,n=1
    assert validate_segment([a("x"), u("q")])       # +1 assistant
    assert validate_segment([a("x"), t("r")])       # m=1,n=0
    assert validate_segment([a("x"), t("r"), t("r2")])  # m=2,n=0
    assert validate_segment([a("x"), u("q"), u("q2")])  # n=2,m=0


# ---------------------------------------------------------------------------
# 不修改输入(纯函数)
# ---------------------------------------------------------------------------


def test_does_not_mutate_input():
    msgs = [u("q1"), a("a1"), u("q2")]
    msgs_copy = [dict(m) for m in msgs]
    split_segments(msgs)
    assert msgs == msgs_copy
