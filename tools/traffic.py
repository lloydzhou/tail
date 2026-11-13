"""一个 agent 会话的**真实**请求序列:不只是纯追加,还含 compact 和分支。

纯追加是最理想的情况,前缀永远有效。真实 agent 跑一晚上会长这样:

  - 大部分轮次是纯追加(工具调用 → 结果 → 再问);
  - 上下文快满时 **compact**:整段历史被摘要替换,前缀从某处开始全变;
  - 用户中途改需求 / 换模型 / 重排消息:同样让前缀失效;
  - 多个会话**并行**(一个开发同时开几个 Session)。

所以这里生成的是「带事件的轨迹」,每个事件标注它属于哪一类,统计时按类别分开看:

======================  ====================================================
append                   纯追加:前缀 100% 有效,只有尾巴上线
new_tools                中途新增工具定义(tools 变了 → 前缀从头就变)
compact                  历史被摘要替换,前 L 字节失效一次
branch                   回退到早先的一轮再往下走(中间消息变了)
parallel-session         另一个会话的请求插进来(凭据表要分得清)
======================  ====================================================

只用标准库和 `_TOOLS`,不依赖 magpie / tail 的任何实现——它描述的是「agent 会发出
什么」,而不是「传输怎么做」。
"""

from __future__ import annotations

import copy
import json
import random
from dataclasses import dataclass, field
from typing import Dict, Iterator, List, Optional

_WORDS = ("分析 数据 结果 模型 输入 输出 评估 指标 样本 训练 推理 上下文 缓存 "
          "传输 协议 分块 网关 客户端 延迟 带宽 命中 前缀 复用 编译 测试 "
          "依赖 构建 部署 日志 告警 索引 事务 锁 队列 编译 语法 重构 提交").split()


def _text(rng: random.Random, n_words: int) -> str:
    return "".join(rng.choice(_WORDS) + ("，" if rng.random() < 0.25 else "")
                   for _ in range(n_words))


def system_prompt(seed: int) -> str:
    """一份长 system prompt:真实 agent 的规则/工具说明就是几 KB 到几十 KB。"""
    rng = random.Random(seed)
    rules = "".join(
        f"{i}. 涉及 {rng.choice(_WORDS)}/{rng.choice(_WORDS)} 时,"
        f"先给出 {rng.choice(_WORDS)} 结论,再说明 {rng.choice(_WORDS)} 依据,"
        f"不要臆造 API;若输入不足请明确列出缺失项。\n"
        for i in range(1, 220))
    return "你是一个严谨的工程助手,在一个大型仓库上工作。\n行为准则:\n" + rules


def tool_defs(extra: int = 0) -> List[Dict]:
    """固定的工具定义;``extra`` 追加几个中途才出现的工具。"""
    base = [
        ("read_file", "读取一个文件的内容(支持行号范围)", [("path", "绝对路径"), ("range", "行号范围")]),
        ("write_file", "写入或覆盖文件", [("path", "路径"), ("content", "内容")]),
        ("run_command", "执行 shell 命令并返回 stdout/stderr", [("command", "命令"), ("cwd", "工作目录")]),
        ("search_code", "在仓库里按正则搜索", [("pattern", "正则"), ("path", "目录")]),
        ("apply_patch", "应用统一 diff 补丁", [("patch", "统一 diff")]),
        ("web_fetch", "抓取网页正文", [("url", "地址")]),
        ("list_dir", "列出目录内容", [("path", "目录")]),
        ("todo_write", "更新任务清单", [("todos", "清单")]),
    ]
    extra_defs = [
        ("spawn_agent", "开一个子 agent 去做子任务", [("task", "任务描述")]),
        ("memory_search", "搜索长期记忆", [("query", "查询")]),
        ("browser_open", "打开浏览器页面", [("url", "地址")]),
    ]
    defs = base + extra_defs[:extra]
    return [
        {"type": "function",
         "function": {"name": name, "description": desc,
                      "parameters": {"type": "object",
                                     "properties": {p: {"type": "string", "description": d}
                                                    for p, d in props},
                                     "required": [p for p, _ in props]}}}
        for name, desc, props in defs
    ]


@dataclass
class Turn:
    """一次要发出去的请求,以及它为什么长这样。"""

    kind: str           # append / new_tools / compact / branch
    session: int        # 哪个会话(并行会话用)
    messages: List[Dict]
    tools: List[Dict]
    note: str = ""

    def payload(self, model: str, stream: bool = False) -> Dict:
        return {"model": model, "messages": self.messages, "tools": self.tools,
                "temperature": 0.3, "stream": stream}


def payload_bytes(payload: Dict) -> bytes:
    """magpie 会发出的那串 JSON 字节(仅用于算「不优化要传多少」)。"""
    return json.dumps(payload, ensure_ascii=False).encode()


class AgentSession:
    """一个会话的消息历史,能追加、能压缩、能回退。"""

    def __init__(self, seed: int, turn_kb: int = 8, system_kb: int = 0) -> None:
        self.rng = random.Random(seed)
        self.turn_words = max(20, int(turn_kb * 1024 / 9))
        self.messages: List[Dict] = [
            {"role": "system", "content": system_prompt(seed)},
        ]
        if system_kb:
            # 再加一份「仓库说明」,把 system 部分撑到几十 KB
            self.messages[0]["content"] += "\n\n仓库说明:\n" + _text(
                self.rng, int(system_kb * 1024 / 9))
        self.turn = 0
        self._checkpoints: List[List[Dict]] = []

    # -- 消息增长 --------------------------------------------------------

    def append_turn(self) -> None:
        rng = self.rng
        i = self.turn
        self.messages.append(
            {"role": "user", "content": f"第{i}步:" + _text(rng, self.turn_words // 4)})
        self.messages.append(
            {"role": "assistant", "content": f"计划{i}:" + _text(rng, self.turn_words // 3),
             "tool_calls": [{
                 "id": f"call_{i}", "type": "function",
                 "function": {"name": "run_command",
                              "arguments": json.dumps(
                                  {"command": f"pytest -k turn{i} -x", "cwd": "/repo"})}}]})
        self.messages.append(
            {"role": "tool", "tool_call_id": f"call_{i}",
             "content": _text(rng, self.turn_words)})
        self.messages.append({"role": "user", "content": "继续。"})
        self.turn += 1

    def checkpoint(self) -> None:
        """记下当前历史,供 branch 事件回退。"""
        self._checkpoints.append(copy.deepcopy(self.messages))

    def branch(self) -> bool:
        """回退到上一个检查点,再换一条路走(中间消息变了)。"""
        if not self._checkpoints:
            return False
        self.messages = copy.deepcopy(self._checkpoints.pop())
        # 换掉最后一条 user:真实场景是「刚才那个方向不对,换个思路」
        for m in reversed(self.messages):
            if m["role"] == "user":
                m["content"] = "换个方向:" + _text(self.rng, self.turn_words // 3)
                break
        return True

    def compact(self) -> str:
        """把历史压成一段摘要:前缀从摘要处开始全变。"""
        head = self.messages[0]                      # system 保留
        body = self.messages[1:]
        if len(body) < 6:
            return ""
        # 一个真实 compact:中间的历史被一句话摘要替换,最近几轮保留
        keep = body[-4:]
        digest = ("之前的工作摘要:" + _text(self.rng, max(60, len(body) * 6)))
        self.messages = [head, {"role": "user", "content": digest}] + keep
        return digest[:40]


@dataclass
class Trace:
    """一段真实的会话轨迹:一串 Turn,外加每轮属于哪一类。"""

    turns: List[Turn] = field(default_factory=list)
    sessions: int = 1

    def __len__(self) -> int:
        return len(self.turns)

    def kinds(self) -> Dict[str, int]:
        out: Dict[str, int] = {}
        for t in self.turns:
            out[t.kind] = out.get(t.kind, 0) + 1
        return out


def build_trace(rounds: int, seed: int = 2026, sessions: int = 1,
                compact_every: int = 0, branch_every: int = 0,
                tools_at: int = 0, turn_kb: int = 8,
                system_kb: int = 0) -> Trace:
    """生成一段轨迹。

    Args:
      rounds: 总轮数(所有会话加起来)
      sessions: 并行会话数,请求按轮转交错发出
      compact_every: 每 N 轮发生一次 compact(0=不发生)
      branch_every: 每 N 轮回退一次再继续(0=不发生)
      tools_at: 第几轮之后追加新工具(0=不追加)
      turn_kb: 每轮新增内容约多少 KB
      system_kb: 额外塞进 system 的仓库说明大小
    """
    trace = Trace(sessions=max(1, sessions))
    pool = [AgentSession(seed + 100 * i, turn_kb=turn_kb, system_kb=system_kb)
            for i in range(max(1, sessions))]
    tools = tool_defs()
    live = list(pool)
    n = 0
    while n < rounds and live:
        i = n % len(live)
        sess = live[i]
        kind, note = "append", ""
        if tools_at and n == tools_at:
            tools = tool_defs(1)                    # 中途多了工具定义
            kind, note = "new_tools", "+1 tool"
        elif compact_every and n and n % compact_every == 0:
            # compact 之前先给一轮纯追加,让「压缩」这一步本身可分辨
            note = sess.compact()
            if note:
                kind = "compact"
        elif branch_every and n and n % branch_every == 0:
            sess.checkpoint()
            sess.append_turn()
            if sess.branch():
                kind = "branch"
        sess.append_turn()
        trace.turns.append(Turn(kind=kind, session=i,
                                messages=copy.deepcopy(sess.messages),
                                tools=copy.deepcopy(tools), note=note))
        n += 1
    return trace


def summarize(trace: Trace) -> Dict[str, object]:
    rows = []
    for t in trace.turns:
        rows.append({"kind": t.kind, "session": t.session,
                     "messages": len(t.messages)})
    return {"turns": len(trace.turns), "sessions": trace.sessions,
            "kinds": trace.kinds(), "rows": rows}
