"""并发会话与网关重启:两个真实 magpie 在「不理想」条件下的行为。

前面所有数据都是单线程串行发的。真实 agent 不长这样:一个开发者同时开几个
Session,请求交错甚至并行;网关也会重启(升级、崩、被调度器挪走)。

这里验证三件会**让数据失真或让功能出错**的事:

1. **并发会话不串台**。不同会话的凭据表互不干扰,推理服务收到的每个会话都完整。
   如果串了,数据会好看得离谱(把别人的前缀当自己的)。
2. **乱序响应不退化**。同一会话有多个请求在飞时,靠后的响应先回来,
   凭据表要保留多条(`maxCredentials`),否则每轮都掉回全量。
3. **网关重启后能自愈**。集群 magpie 被杀掉重起,块缓存空了 —— 客户端应当收到
   409、丢掉凭据、全量重发一次,而**上层 agent 什么都不知道**。

用法::

    .venv/bin/python -m tools.hardcases --magpie /tmp/magpie-tail --check all
"""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
from typing import Dict, List, Optional

import httpx2 as httpx

from .link import ThrottledRelay
from .magpie_pair import Server, _free_port
from .realworld import Magpie
from .traffic import AgentSession, payload_bytes


def payload(model: str, messages: List[Dict], stream: bool = False) -> Dict:
    """这一组工具里**唯一**的请求形状。

    会话凭据是按渲染正文的第一个块哈希分组的(`Session`),所以同一会话的每个
    请求渲染出来必须是同一个前缀。请求形状(有没有 tools/temperature/stream)
    会改变渲染正文的开头 —— 形状不一致 = 换了会话 = 永远没有凭据可复用。
    这里只有这一个构造函数,就是为了不给自己埋这个坑。
    """
    return {"model": model, "messages": messages, "tools": [],
            "temperature": 0.3, "stream": stream}


def ask(url: str, model: str, session: AgentSession, stream: bool = False) -> Dict:
    with httpx.Client(timeout=300) as c:
        r = c.post(url, content=payload_bytes(payload(model, session.messages, stream)),
                   headers={"content-type": "application/json",
                            "authorization": "Bearer magpie"})
        r.raise_for_status()
    return r.json()


# ---------------------------------------------------------------------------
# 检查 1+2:并发会话 / 乱序响应
# ---------------------------------------------------------------------------

def check_parallel_sessions(url: str, tail_model: str, received: List[Dict],
                            base: int, n_sessions: int, rounds: int) -> Dict:
    """N 个会话各跑几轮,全部并发,验证不串台、每条都完整。"""
    sessions = [AgentSession(seed=7000 + i, turn_kb=12) for i in range(n_sessions)]
    expected: List[List[Dict]] = []
    for _ in range(rounds):
        for i, s in enumerate(sessions):
            s.append_turn()
            expected.append([dict(m) for m in s.messages])

    # 并发发:同一时刻多个会话的请求在飞(乱序响应就有了)
    errors: List[str] = []
    def worker(idx: int) -> None:
        s = sessions[idx % n_sessions]
        try:
            ask(url, tail_model, s)
        except Exception as exc:                # noqa: BLE001 — 收集起来一起报
            errors.append(f"session {idx}: {exc}")

    t0 = time.perf_counter()
    with concurrent.futures.ThreadPoolExecutor(max_workers=n_sessions) as pool:
        list(pool.map(worker, range(len(expected))))
    elapsed = time.perf_counter() - t0

    # received 是 list,必须从原列表按起点切片(list[:] 拿到的是副本,
    # 之后 append 进来的条目不会出现在副本里)
    seen = received[base:]
    # 上游收到的每一条,都必须等于某个会话某一轮的完整历史。
    # 注意比对口径:magpie 会在转发前改写 body(工具形状、thinking 字段等),
    # 所以不能比整串 JSON,只能比「这些 messages 里是否正好是某一个会话的历史」:
    # 逐条比对 role/content,并要求条数一致。
    want = [e for e in expected]
    bad = []
    for r in seen:
        got = r["messages"]
        ok = any(len(got) == len(w) and
                 all(g.get("role") == x.get("role") and
                     str(g.get("content")) == str(x.get("content"))
                     for g, x in zip(got, w))
                 for w in want)
        if not ok:
            bad.append(got)
    return {"sent": len(expected), "received": len(seen),
            "errors": errors, "corrupted": len(bad), "seconds": round(elapsed, 2),
            "want": len(want)}


def _drain(relay: "ThrottledRelay", quiet: float = 0.6) -> None:
    """把 relay 里残留的字节彻底排空。

    `take_windows` 在静默期内会把**最后一段**挂起留到下次调用,所以「调一次」
    不等于「清空」:并发阶段那一堆字节的尾巴会被算进下一次测量,量出来的数字
    就变成「残留 + 这一轮」,看起来像退化、其实是我自己数错。这里反复取到空为止。
    """
    for _ in range(10):
        time.sleep(quiet)
        if not relay.take_windows(gap=0.05, settle=quiet):
            return
    raise AssertionError("relay 一直有残留字节,排不空")


def _measure_upload(url: str, model: str, session: AgentSession,
                    relay: "ThrottledRelay", timeout: float = 40.0) -> int:
    """发一轮,返回这一轮上行到集群 magpie 的字节数(从 relay 实测)。"""
    _drain(relay)
    ask(url, model, session)
    deadline = time.time() + timeout
    windows = []
    while time.time() < deadline:
        windows = relay.take_windows(gap=0.08, settle=0.6)
        if windows:
            break
        time.sleep(0.05)
    if not windows:
        raise AssertionError("没量到这一轮的上行字节")
    return sum(w.up_bytes for w in windows)


def check_out_of_order(url: str, plain_model: str, tail_model: str,
                       relay: "ThrottledRelay", rounds: int) -> Dict:
    """同一会话的多个请求同时在飞,且**最早(历史最短)的那个最后回来**。

    凭据表每个会话留 4 条(`maxCredentials`)就是为了这个。只留 1 条的话,
    最后回来的那个响应会把凭据覆盖成**最短**的那个 —— 下一轮仍然「能用」、
    仍然有命中,只是复用的前缀一下子缩回到最短那份,多上行几十 KB,而且
    什么都不报错。这种退化在曲线图上看不出来(还是省了很多),但省下的部分
    被悄悄吐回去了。

    所以这里量的是字节数,不是比值:构造「最短的响应最后到」,再串行发一轮,
    用中继实测这一轮上行多少,并和「按**最长**那一份复用」的算术值比对。
    只有多凭据被保下来,才可能命中最长的那份。
    """
    from tail.transport.render import render_request

    n_rounds = rounds
    out: Dict[str, object] = {"inflight": n_rounds}

    session = AgentSession(seed=4242, turn_kb=16)
    payloads: List[bytes] = []
    for _ in range(n_rounds):
        session.append_turn()
        payloads.append(payload_bytes(payload(tail_model, session.messages)))

    errors: List[str] = []

    def fire(i: int, delay: float) -> None:
        try:
            time.sleep(delay)
            with httpx.Client(timeout=300) as c:
                r = c.post(url, content=payloads[i],
                           headers={"content-type": "application/json",
                                    "authorization": "Bearer magpie"})
                r.raise_for_status()
        except Exception as exc:                # noqa: BLE001
            errors.append(f"{i}: {exc}")

    # 第 0 个(历史最短)压后发,保证它的响应**最后**回来
    stagger = [0.25] + [0.0] * (n_rounds - 1)
    with concurrent.futures.ThreadPoolExecutor(max_workers=n_rounds) as pool:
        list(pool.map(lambda i: fire(i, stagger[i]), range(n_rounds)))
    relay.take_windows(gap=0.05, settle=0.6)     # 并发阶段的不算数,量之前会排空

    # 串行再发一轮,实测上行字节
    session.append_turn()
    body = payload_bytes(payload(tail_model, session.messages))
    measured = _measure_upload(url, tail_model, session, relay)
    plain_up = _measure_upload(url, plain_model, session, relay)

    # 算术基准:这一轮能复用的前缀,就是**上一轮(最长那份)响应确认过的**长度,
    # 即上一轮渲染正文向下取整到 512 的块边界。
    full = len(render_request(json.loads(body)))
    prev = len(render_request(json.loads(payloads[-1])))
    best_have = (prev // 512) * 512
    worst_have = (len(render_request(json.loads(payloads[0]))) // 512) * 512
    expect = full - best_have
    # 实测含 HTTP 头与分块成帧,给 1 KB 余量
    headroom = 1024

    out.update({
        "measured": measured,
        "plain_upload": plain_up,
        "full_text": full,
        "expect_tail": expect,
        "if_single_cred": full - worst_have,
        "ratio": round(plain_up / max(measured, 1), 2),
        "errors": errors,
    })
    if errors:
        out["verdict"] = f"请求出错:{errors[:2]}"
    elif measured > expect + headroom:
        out["verdict"] = (
            f"退化:只复用了很短的前缀(实测 {measured} B,"
            f"按最长复用应为 ~{expect} B,只留 1 条凭据会是 ~{full - worst_have} B)")
    elif measured >= plain_up:
        out["verdict"] = f"没省:{measured} B vs plain {plain_up} B"
    else:
        out["verdict"] = "ok"
    return out


# ---------------------------------------------------------------------------
# 检查 3:网关重启
# ---------------------------------------------------------------------------

def check_restart(cluster: Magpie, url: str, tail_model: str, session: AgentSession,
                  relay: "ThrottledRelay", rounds_before: int,
                  rounds_after: int) -> Dict:
    """跑几轮 → 杀掉集群 magpie 重起(块缓存空了)→ 再跑几轮。

    「重起后还能用」不够:缓存空了的时候,客户端手里那套凭据已经全是假的。
    这时应当是**恰好浪费一轮** —— 集群回 409,客户端丢掉凭据全量重发,上层
    agent 什么都不知道 —— 从第二轮起重新只上行尾巴。如果每一轮都全量,那是
    「能跑但再也没省过」;如果一轮都没浪费,那说明凭据根本没在用。

    所以这里量每一轮的上行字节,并对「第一轮 ≈ 全量、后面几轮 ≤ 一半」下断言。
    通不通过看字节数,不看请求有没有报错。
    """
    from tail.transport.render import render_request

    for _ in range(rounds_before):
        session.append_turn()
        ask(url, tail_model, session)

    cluster.stop()
    cluster.start()
    cluster.wait()
    # 重起后是同一个端口、同一份配置;块缓存是内存的,已经空了

    rows: List[Dict] = []
    recovered = 0
    full_texts: List[int] = []
    for _ in range(rounds_after):
        session.append_turn()
        body = payload_bytes(payload(tail_model, session.messages))
        full = len(render_request(json.loads(body)))
        up = _measure_upload(url, tail_model, session, relay)
        rows.append({"full_text": full, "upload": up})
        full_texts.append(full)
        if up <= full * 0.5:
            recovered += 1

    first_up, first_full = rows[0]["upload"], rows[0]["full_text"]
    # 第一轮里装了两趟:先按旧凭据上行一条尾巴(被 409 拒掉,白花),
    # 再全量重发一次。所以它应当 >= 全量,超出的那部分正是白花掉的尾巴。
    wasted_one_round = first_up >= first_full
    later_partial = recovered >= 1 and all(r["upload"] <= r["full_text"] * 0.5
                                           for r in rows[1:])
    if wasted_one_round and later_partial:
        verdict = "ok"
    elif not wasted_one_round:
        verdict = (f"可疑:重起后第一轮只上行 {first_up}/{first_full} B,"
                   f"不像「缓存空了再全量重发一次」")
    else:
        verdict = ("退化:重起后一直没能恢复复用:"
                   + ", ".join(f"{r['upload']}/{r['full_text']}" for r in rows))
    return {"recovered": recovered, "of": len(rows), "rows": rows,
            "verdict": verdict,
            "wasted_tail": max(first_up - first_full, 0)}


# ---------------------------------------------------------------------------
# 主流程
# ---------------------------------------------------------------------------

def main() -> None:
    ap = argparse.ArgumentParser(description="两个真 magpie 的硬场景")
    ap.add_argument("--magpie", required=True)
    ap.add_argument("--check", default="all",
                    choices=["all", "parallel", "outoforder", "restart"])
    ap.add_argument("--sessions", type=int, default=4)
    ap.add_argument("--rounds", type=int, default=3)
    ap.add_argument("--json-out", default="")
    ap.add_argument("--keep", action="store_true")
    args = ap.parse_args()
    if not os.access(args.magpie, os.X_OK):
        sys.exit(f"{args.magpie} 不可执行")

    root = tempfile.mkdtemp(prefix="magpie-hard-")
    from tests.mock_backend import build_backend_app
    backend_app = build_backend_app()
    backend_port = _free_port()
    servers = [Server(backend_app, backend_port, "mock 推理服务")]
    cluster = Magpie(args.magpie, os.path.join(root, "cluster"),
                     f"127.0.0.1:{_free_port()}", tail=True)
    relay: Optional[ThrottledRelay] = None
    local: Optional[Magpie] = None
    try:
        cluster.provider("mock", f"http://127.0.0.1:{backend_port}/v1",
                         "deepseek-chat", tail=False)
        cluster.start()
        cluster.wait()
        cluster_model = [m for m in cluster.models() if m.startswith("mock/")][0]

        # 本地 → 集群这一段串一个中继,用来实测每轮到底上行多少字节。
        # 不限速:这里量的是字节数,不是时间(时间由 tools/realworld.py 量)。
        relay = ThrottledRelay("127.0.0.1",
                               int(cluster.addr.rsplit(":", 1)[1]), mbps=0)
        relay.start()
        peer = f"http://127.0.0.1:{relay.port}/v1"

        local = Magpie(args.magpie, os.path.join(root, "local"),
                       f"127.0.0.1:{_free_port()}")
        local.provider("plain", peer, cluster_model, tail=False)
        local.provider("tail", peer, cluster_model, tail=True)
        local.start()
        local.wait()
        models = local.models()
        plain_model = [m for m in models if m.startswith("plain/")][0]
        tail_model = [m for m in models if m.startswith("tail/")][0]
        url = f"http://{local.addr}/v1/chat/completions"
        print(f"本地 magpie {local.addr} → 集群 magpie {cluster.addr} → 推理服务")
        print(f"模型 {tail_model}")

        result: Dict[str, object] = {}
        if args.check in ("all", "parallel"):
            # 用 backend_app.state.received 作为「上游收到了什么」的观测点
            seen = backend_app.state.received
            out = check_parallel_sessions(url, tail_model, seen, len(seen),
                                          args.sessions, args.rounds)
            result["parallel"] = out
            print(f"\n[并发] {out['sent']} 个请求 / {args.sessions} 个会话并行:"
                  f"上游收到 {out['received']},串台 {out['corrupted']},"
                  f"错误 {len(out['errors'])},用时 {out['seconds']}s")
            if out["errors"]:
                print("  错误示例:", out["errors"][:2])

        if args.check in ("all", "outoforder"):
            out = check_out_of_order(url, plain_model, tail_model, relay,
                                     args.rounds + 1)
            result["outoforder"] = out
            print(f"\n[乱序] 同一会话 {out['inflight']} 个请求同时在飞(最短的那个最后回),"
                  f"再串行一轮:")
            print(f"  上行 tail {out['measured']} B vs plain {out['plain_upload']} B"
                  f"  ({out['ratio']}x)")
            print(f"  全量渲染正文 {out['full_text']} B;按最长复用应上行 ~{out['expect_tail']} B,"
                  f"只留 1 条凭据则 ~{out['if_single_cred']} B")
            print(f"  → {out['verdict']}")
            if out["errors"]:
                print("  错误示例:", out["errors"][:2])

        if args.check in ("all", "restart"):
            session = AgentSession(seed=9999, turn_kb=12)
            out = check_restart(cluster, url, tail_model, session, relay,
                                rounds_before=3, rounds_after=4)
            result["restart"] = out
            print(f"\n[重启] 集群 magpie 被杀掉重起后,"
                  f"{out['recovered']}/{out['of']} 轮只上行尾巴:")
            for i, r in enumerate(out["rows"], 1):
                note = ""
                if i == 1:
                    note = (f"  ← 含一次被 409 拒掉的尾巴"
                            f" {out['wasted_tail']} B + 一次全量重发")
                print(f"  第 {i} 轮 {r['upload']:>7} B / 全量 {r['full_text']} B{note}")
            print(f"  → {out['verdict']}")

        if args.json_out:
            with open(args.json_out, "w") as f:
                json.dump(result, f, ensure_ascii=False, indent=2)
            print(f"已写 {args.json_out}")
    finally:
        for m in (relay, local, cluster):
            if m is not None:
                m.stop()
        for s in servers:
            s.stop()
        if not args.keep:
            shutil.rmtree(root, ignore_errors=True)


if __name__ == "__main__":
    main()
