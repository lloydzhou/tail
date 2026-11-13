"""两个 magpie 之间真机取数:原生 JSON 上行 vs tail 上行。

拓扑和线上部署完全一致,只是把「集群」放到 127.0.0.1 上::

    ┌──────────────┐   HTTP    ┌──────────────┐   HTTP   ┌──────────────┐
    │ 本脚本       │──────────▶│ tail 客户端  │─────────▶│ tail 网关    │──▶ mock 推理
    │ (扮演 magpie)│  JSON 或  │ 代理 proxy   │ tail v3  │ serve        │
    └──────────────┘  tail v3  └──────────────┘          └──────────────┘
                                                              │ 块缓存
                                                              ▼ (Redis / 内存)

两种模式跑同一个「agent 会话」:system + tools 固定,每轮追加 user / assistant
tool_call / tool 结果(过去的消息不变,只有尾部增长——这正是前缀可复用的前提)。

  native —— magpie 直连,整包 JSON 上行(今天的默认行为)
  tail   —— magpie → 本地代理 → 集群网关,只上行尾巴字节

每次请求都校验推理服务收到的 messages 与 magpie 发出的逐条一致。

用法::

    .venv/bin/python -m tools.magpie_pair                      # 内存存储
    .venv/bin/python -m tools.magpie_pair --redis redis://127.0.0.1:6390/0
    .venv/bin/python -m tools.magpie_pair --rounds 12 --json-out /tmp/pair.json

输出:逐轮的上行字节 / 耗时 / 代理准备耗时;把实测字节代进 10/50/200 Mbps 上行
带宽的上传耗时(**算出来的,不是测出来的**,表头会标注);以及用实测「省下的字节」
和实测「多花的 CPU」反推的盈亏平衡带宽。
"""

from __future__ import annotations

import argparse
import json
import random
import socket
import threading
import time
from typing import Dict, List, Tuple

import httpx2 as httpx
import uvicorn

from tail import proxy, serve
from tail.transport import storage

# ---------------------------------------------------------------------------
# 一个「像真的」agent 会话
# ---------------------------------------------------------------------------

_WORDS = ("分析 数据 结果 模型 输入 输出 评估 指标 样本 训练 推理 上下文 缓存 "
          "传输 协议 分块 网关 客户端 延迟 带宽 命中 前缀 复用 编译 测试 "
          "依赖 构建 部署 日志 告警 索引 事务 锁 队列").split()

_TOOLS = [
    {
        "type": "function",
        "function": {
            "name": name,
            "description": desc,
            "parameters": {"type": "object", "properties": {
                p: {"type": "string", "description": d} for p, d in props},
                "required": [p for p, _ in props]},
        },
    }
    for name, desc, props in [
        ("read_file", "读取一个文件的内容(支持行号范围)", [("path", "绝对路径"), ("range", "行号范围")]),
        ("write_file", "写入或覆盖文件", [("path", "路径"), ("content", "内容")]),
        ("run_command", "执行 shell 命令并返回 stdout/stderr", [("command", "命令"), ("cwd", "工作目录")]),
        ("search_code", "在仓库里按正则搜索", [("pattern", "正则"), ("path", "目录")]),
        ("apply_patch", "应用统一 diff 补丁", [("patch", "统一 diff")]),
        ("web_fetch", "抓取网页正文", [("url", "地址")]),
        ("list_dir", "列出目录内容", [("path", "目录")]),
        ("todo_write", "更新任务清单", [("todos", "清单")]),
    ]
]


def _text(rng: random.Random, n_words: int) -> str:
    return "".join(rng.choice(_WORDS) + ("，" if rng.random() < 0.25 else "")
                   for _ in range(n_words))


def _system_prompt(seed: int) -> str:
    rng = random.Random(seed)
    rules = "".join(
        f"{i}. 涉及 {rng.choice(_WORDS)}/{rng.choice(_WORDS)} 时,"
        f"先给出 {rng.choice(_WORDS)} 结论,再说明 {rng.choice(_WORDS)} 依据,"
        f"不要臆造 API;若输入不足请明确列出缺失项。\n"
        for i in range(1, 220))
    return ("你是一个严谨的工程助手,在一个大型仓库上工作。\n"
            "行为准则:\n" + rules)


class Session:
    """一个 agent 会话:过去的消息不变,每轮只在尾部追加。"""

    def __init__(self, seed: int = 2026, turn_kb: int = 8) -> None:
        self.rng = random.Random(seed)
        self.turn_words = max(20, int(turn_kb * 1024 / 9))  # 一个汉字约 3 B
        self.messages: List[Dict] = [
            {"role": "system", "content": _system_prompt(seed)},
        ]
        self.turn = 0

    def advance(self) -> List[Dict]:
        """模拟 magpie 组下一轮请求:追加 user / assistant(tool_call) / tool。"""
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
        self.messages.append(
            {"role": "user", "content": "继续。"})
        self.turn += 1
        return self.messages

    def payload(self, stream: bool = False) -> Dict:
        return {"model": "deepseek-chat", "messages": self.messages,
                "tools": _TOOLS, "temperature": 0.3, "stream": stream}


def render_bytes(payload: Dict) -> bytes:
    return json.dumps(payload, ensure_ascii=False).encode()


# ---------------------------------------------------------------------------
# 起服务
# ---------------------------------------------------------------------------

def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class Server:
    def __init__(self, app, port: int, name: str) -> None:
        self.name = name
        self.port = port
        config = uvicorn.Config(app, host="127.0.0.1", port=port,
                                log_level="error", lifespan="off",
                                access_log=False)
        self.server = uvicorn.Server(config)
        self.thread = threading.Thread(target=self.server.run, daemon=True)
        self.thread.start()
        deadline = time.time() + 20
        while not self.server.started:
            if time.time() > deadline:
                raise RuntimeError(f"{name} 启动超时")
            if not self.thread.is_alive():
                raise RuntimeError(f"{name} 线程退出")
            time.sleep(0.01)

    def stop(self) -> None:
        self.server.should_exit = True
        self.thread.join(timeout=5)


def start_cluster(redis: str) -> dict:
    """起 mock 推理服务 + tail 网关(集群侧)+ tail 客户端代理(本地侧)。"""
    from tests.mock_backend import build_backend_app

    backend = build_backend_app()
    backend_port, gateway_port, proxy_port = _free_port(), _free_port(), _free_port()

    store = storage.make_store(redis or "", ttl=3600)
    if redis:
        store.clear()

    gateway_app = serve.build_app(f"http://127.0.0.1:{backend_port}", store)
    proxy_app = proxy.build_app(f"http://127.0.0.1:{gateway_port}")

    servers = [
        Server(backend, backend_port, "mock 推理服务"),
        Server(gateway_app, gateway_port, "tail 网关(集群侧)"),
        Server(proxy_app, proxy_port, "tail 代理(本地侧)"),
    ]
    return {
        "servers": servers,
        "backend_app": backend,
        "backend_url": f"http://127.0.0.1:{backend_port}",
        "gateway_url": f"http://127.0.0.1:{gateway_port}",
        "proxy_url": f"http://127.0.0.1:{proxy_port}",
        "store": store,
        "proxy_stats": proxy_app.state.stats,
    }


# ---------------------------------------------------------------------------
# 一轮请求
# ---------------------------------------------------------------------------

def one_call(url: str, payload: Dict) -> Dict:
    raw = render_bytes(payload)
    t0 = time.perf_counter()
    first = None
    with httpx.Client(timeout=300) as client:
        with client.stream("POST", url + "/v1/chat/completions", content=raw,
                           headers={"content-type": "application/json"}) as r:
            r.raise_for_status()
            for chunk in r.iter_bytes():
                if first is None and chunk:
                    first = time.perf_counter()
    done = time.perf_counter()
    return {"bytes": len(raw), "ttfb_ms": ((first or done) - t0) * 1000,
            "total_ms": (done - t0) * 1000}


def run_pass(base_url: str, stats, rounds: int, stream: bool) -> List[Dict]:
    """跑一遍完整会话,逐轮记录上行字节与耗时。"""
    rows: List[Dict] = []
    session = Session()
    for _ in range(rounds):
        session.advance()
        payload = session.payload(stream)
        before = stats.as_dict() if stats else None
        result = one_call(base_url, payload)
        after = stats.as_dict() if stats else None
        if stats:
            wire = after["wire_bytes_out"] - before["wire_bytes_out"]
            prep = after["prepare_ms"] - before["prepare_ms"]
            hit = after["hits"] - before["hits"]
        else:
            wire, prep, hit = result["bytes"], 0.0, 0
        rows.append({"round": len(rows) + 1, "json_bytes": result["bytes"],
                     "wire_bytes": wire, "ttfb_ms": round(result["ttfb_ms"], 2),
                     "total_ms": round(result["total_ms"], 2),
                     "prepare_ms": round(prep, 2), "hit": bool(hit)})
    return rows


def check_fidelity(cluster: dict, rounds: int, stream: bool) -> None:
    """推理服务收到的 messages 必须与 magpie 发出的逐条一致。"""
    received = cluster["backend_app"].state.received[-rounds:]
    session = Session()
    expected = [session.advance()[:] for _ in range(rounds)]
    assert len(received) == rounds, (len(received), rounds)
    for i, (got, want) in enumerate(zip(received, expected)):
        if got["messages"] != want:
            raise AssertionError(f"第 {i + 1} 轮拼装结果与 magpie 发出的不一致")


# ---------------------------------------------------------------------------
# 报表
# ---------------------------------------------------------------------------

def _kb(n: float) -> str:
    return f"{n / 1024:.1f}"


def _mb(n: float) -> str:
    return f"{n / 1024 / 1024:.2f}"


def report(native: List[Dict], tail: List[Dict], stats: Dict) -> dict:
    rounds = len(native)
    print()
    print("=" * 94)
    print(f"{'轮':>3} {'JSON上行':>10} {'原生耗时':>10} {'tail上行':>11} "
          f"{'tail耗时':>10} {'代理准备':>9} {'省':>7}")
    print(f"{'':>3} {'(KB)':>10} {'(ms)':>10} {'(KB)':>11} {'(ms)':>10} "
          f"{'(ms)':>9} {'(倍)':>7}")
    print("-" * 94)
    for a, b in zip(native, tail):
        ratio = a["json_bytes"] / b["wire_bytes"] if b["wire_bytes"] else 0
        print(f"{a['round']:>3} {_kb(a['json_bytes']):>10} {a['total_ms']:>10.1f} "
              f"{_kb(b['wire_bytes']):>11} {b['total_ms']:>10.1f} "
              f"{b['prepare_ms']:>9.1f} {ratio:>6.1f}x")
    print("-" * 94)

    n_json = sum(r["json_bytes"] for r in native)
    t_wire = sum(r["wire_bytes"] for r in tail)
    n_total = sum(r["total_ms"] for r in native)
    t_total = sum(r["total_ms"] for r in tail)
    prep = stats["prepare_ms"]
    print(f"合计 {rounds} 轮:原生上行 {_mb(n_json)} MB / {n_total / 1000:.2f} s ;"
          f" tail 上行 {_mb(t_wire)} MB / {t_total / 1000:.2f} s ;"
          f" 体积 {n_json / t_wire:.1f}x")
    print(f"代理侧准备(渲染+算块+自检)合计 {prep:.1f} ms,均摊 {prep / rounds:.2f} ms/轮")
    print(f"本地回环端到端:原生 {n_total / rounds:.1f} ms/轮,tail {t_total / rounds:.1f} ms/轮,"
          f"差 {(t_total - n_total) / rounds:+.1f} ms/轮"
          f"(本地没有链路成本,tail 这边只剩多一跳 + 编解码的 CPU)")

    # —— 把实测字节代进真实上行带宽:算出来的,不是测出来的 ——
    print()
    print("【模型】按实测字节换算的累计上传耗时(上行带宽,不含服务端处理):")
    print(f"{'上行带宽':>10} {'原生(s)':>9} {'tail(s)':>9} {'省(s)':>8} {'省':>6}")
    wan = {}
    for mbps in (10, 50, 200, 1000):
        n_s = n_json * 8 / (mbps * 1e6)
        t_s = t_wire * 8 / (mbps * 1e6)
        wan[mbps] = {"native_s": n_s, "tail_s": t_s, "saved_s": n_s - t_s}
        print(f"{mbps:>7} Mbps {n_s:>9.2f} {t_s:>9.2f} {n_s - t_s:>8.2f} "
              f"{n_s / t_s:>5.1f}x")
    print(f"(口径:{rounds} 轮合计,即这个会话从头跑到第 {rounds} 轮累计上传的字节)")

    # —— 用实测「省下的字节」/「多花的 CPU」反推盈亏平衡带宽 ——
    print()
    print("【实测】每轮:省下的上传 vs 多花的 CPU → 盈亏平衡带宽")
    rows = []
    print(f"{'轮':>3} {'省下(KB)':>10} {'多花(ms)':>10} {'平衡带宽':>12}")
    for a, b in zip(native, tail):
        saved = a["json_bytes"] - b["wire_bytes"]
        extra = b["total_ms"] - a["total_ms"]
        if saved <= 0:
            continue
        if extra <= 0:
            be = "0(负开销)"
        else:
            be = f"{saved * 8 / (extra / 1000) / 1e6:.0f} Mbps"
        rows.append({"round": a["round"], "saved_bytes": saved,
                     "extra_ms": round(extra, 2), "break_even": be})
        print(f"{a['round']:>3} {_kb(saved):>10} {extra:>10.2f} {be:>12}")
    print("(上行带宽高于这个值,tail 的 CPU 开销就盖不过省的传输时间;低于它就有正收益。"
          "回环测的是 CPU,tail 实际还多一跳 RTT——这一跳在真实链路里本来也在。)")
    return {"rounds": rounds, "native": native, "tail": tail,
            "totals": {"json_bytes": n_json, "wire_bytes": t_wire,
                       "native_ms": round(n_total, 1), "tail_ms": round(t_total, 1),
                       "ratio": round(n_json / t_wire, 2),
                       "prepare_ms": round(prep, 2)},
            "wan_model": {str(k): {kk: round(vv, 4) for kk, vv in v.items()}
                          for k, v in wan.items()},
            "break_even": rows, "gateway_stats": stats}


def main() -> None:
    ap = argparse.ArgumentParser(description="两个 magpie 之间的上行对比")
    ap.add_argument("--rounds", type=int, default=10)
    ap.add_argument("--redis", default="", help="块存储;空=进程内内存")
    ap.add_argument("--stream", action="store_true", help="让推理服务走 SSE")
    ap.add_argument("--json-out", default="", help="把结果写成 JSON")
    args = ap.parse_args()

    cluster = start_cluster(args.redis)
    try:
        print("已起:" + " / ".join(f"{s.name} :{s.port}" for s in cluster["servers"]))
        print(f"块存储: {'Redis ' + args.redis if args.redis else '进程内内存'}")

        print("\n[1/2] 原生 JSON 上行(magpie 直连推理服务)…")
        native = run_pass(cluster["backend_url"], None, args.rounds, args.stream)

        print("[2/2] tail 上行(magpie → 本地代理 → 集群网关 → 推理服务)…")
        cluster["backend_app"].state.received.clear()
        stats = cluster["proxy_stats"]
        stats.reset()
        tail = run_pass(cluster["proxy_url"], stats, args.rounds, args.stream)

        check_fidelity(cluster, args.rounds, args.stream)
        result = report(native, tail, stats.as_dict())
        print("\n推理服务收到的 messages 与 magpie 发出的逐条一致 ✓")

        if args.json_out:
            with open(args.json_out, "w") as f:
                json.dump(result, f, ensure_ascii=False, indent=2)
            print(f"已写 {args.json_out}")
    finally:
        for s in cluster["servers"]:
            s.stop()


if __name__ == "__main__":
    main()
