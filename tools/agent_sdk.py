"""用**真实 agent SDK**经两个 magpie 跑一遍,证明「接到哪个 agent 都无所谓」。

前面所有数据都是用裸 HTTP 发的。这个脚本改用官方 `openai` SDK(agent 们实际用的
就是这个)对着**本地那个 magpie** 说话,让它自己决定怎么把请求发到远端:

    openai.OpenAI  ──▶  magpie(本地)  ──▶  限速链路  ──▶  magpie(集群) ──▶ 推理服务
                        ├── provider plain
                        └── provider tail=on

客户端代码两种模式**一字不差**,只有 `model=` 里的 provider 名不同:

    client = OpenAI(base_url="http://127.0.0.1:PORT/v1", api_key="magpie")
    stream = client.chat.completions.create(model="plain/<upstream>" …)
    stream = client.chat.completions.create(model="tail/<upstream>"  …)

这正好回答「我把 api 接到哪个 agent 使用其实没有区别」:SDK 侧零改动,省的是两个
magpie 之间那一段。

用法::

    .venv/bin/python -m tools.agent_sdk --magpie /tmp/magpie-tail --rounds 8 --mbps 25
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
import tempfile
import time
from typing import Dict, List, Optional

import httpx2 as httpx

from .link import ThrottledRelay, upload_seconds
from .magpie_pair import Server, _free_port
from .realworld import Magpie, _kb, _mb
from .traffic import Turn, build_trace

from openai import OpenAI


def make_client(base_url: str) -> OpenAI:
    """agent 会写的那一行——两种模式完全一样。"""
    return OpenAI(base_url=base_url, api_key="magpie", timeout=600,
                  http_client=httpx.Client(timeout=600))


def ask_streaming(client: OpenAI, model: str, turn: Turn) -> str:
    """按 agent 的标准用法发一轮,读流式回复。"""
    out = []
    stream = client.chat.completions.create(
        model=model,
        messages=turn.messages,
        tools=turn.tools,
        temperature=0.3,
        stream=True,
    )
    for event in stream:
        if not event.choices:
            continue
        delta = event.choices[0].delta
        if getattr(delta, "content", None):
            out.append(delta.content)
    return "".join(out)


def run_pass(client: OpenAI, model: str, trace, relay: ThrottledRelay) -> List[Dict]:
    rows: List[Dict] = []
    relay.reset()
    for turn in trace.turns:
        t0 = time.perf_counter()
        text = ask_streaming(client, model, turn)
        ms = (time.perf_counter() - t0) * 1000
        windows = []
        for _ in range(40):
            windows = relay.take_windows(settle=0.0)
            if windows:
                break
            time.sleep(0.002)
        rows.append({"kind": turn.kind,
                     "wire_bytes": sum(w.up_bytes for w in windows),
                     "upload_ms": round(max((w.seconds for w in windows),
                                            default=0.0) * 1000, 1),
                     "total_ms": round(ms, 1),
                     "reply_chars": len(text)})
    return rows


def main() -> None:
    ap = argparse.ArgumentParser(description="真实 openai SDK 经两个 magpie")
    ap.add_argument("--magpie", required=True)
    ap.add_argument("--rounds", type=int, default=8)
    ap.add_argument("--mbps", type=float, default=25.0)
    ap.add_argument("--turn-kb", type=int, default=16)
    ap.add_argument("--system-kb", type=int, default=40)
    ap.add_argument("--json-out", default="")
    ap.add_argument("--keep", action="store_true")
    args = ap.parse_args()
    if not os.access(args.magpie, os.X_OK):
        sys.exit(f"{args.magpie} 不可执行")

    root = tempfile.mkdtemp(prefix="magpie-sdk-")
    from tests.mock_backend import build_backend_app
    backend = build_backend_app()
    backend_port = _free_port()
    servers = [Server(backend, backend_port, "mock 推理服务")]
    cluster = Magpie(args.magpie, os.path.join(root, "cluster"),
                     f"127.0.0.1:{_free_port()}", tail=True)
    relay: Optional[ThrottledRelay] = None
    local: Optional[Magpie] = None
    try:
        trace = build_trace(args.rounds, turn_kb=args.turn_kb,
                            system_kb=args.system_kb)
        print(f"轨迹:{len(trace)} 轮,均纯追加 {trace.kinds()}")

        cluster.provider("mock", f"http://127.0.0.1:{backend_port}/v1",
                         "deepseek-chat", tail=False)
        cluster.start()
        cluster.wait()
        cluster_model = [m for m in cluster.models() if m.startswith("mock/")][0]

        relay = ThrottledRelay("127.0.0.1",
                               int(cluster.addr.rsplit(":", 1)[1]),
                               mbps=args.mbps)
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

        base_url = f"http://{local.addr}/v1"
        print(f"openai SDK → {base_url},模型 {plain_model} / {tail_model},"
              f"链路 {args.mbps} Mbps")

        # 客户端代码一字不差,只有 model 不同
        client = make_client(base_url)
        print("\n[1/2] plain…")
        plain = run_pass(client, plain_model, trace, relay)
        print("[2/2] tail…")
        tail = run_pass(client, tail_model, trace, relay)

        seen = backend.state.received[-2 * len(trace):]
        if len(seen) == 2 * len(trace):
            for i, (a, b) in enumerate(zip(seen[:len(trace)], seen[len(trace):])):
                if a["messages"] != b["messages"]:
                    raise AssertionError(f"第 {i+1} 轮:两条路的消息不一致")
            print("\n推理服务收到的 messages 两条路逐轮一致 ✓")

        print()
        print("=" * 84)
        print(f"{'轮':>3} {'plain上行':>11} {'plain上传':>10} {'tail上行':>11} "
              f"{'tail上传':>10} {'省':>7} {'回复':>6}")
        print(f"{'':>3} {'(KB)':>11} {'(ms)':>10} {'(KB)':>11} {'(ms)':>10} "
              f"{'(倍)':>7} {'(字)':>6}")
        print("-" * 84)
        for i, (a, b) in enumerate(zip(plain, tail), 1):
            ratio = a["wire_bytes"] / b["wire_bytes"] if b["wire_bytes"] else 0
            print(f"{i:>3} {_kb(a['wire_bytes']):>11} {a['upload_ms']:>10.1f} "
                  f"{_kb(b['wire_bytes']):>11} {b['upload_ms']:>10.1f} "
                  f"{ratio:>6.1f}x {a['reply_chars']:>6}")
        print("-" * 84)
        pw = sum(r["wire_bytes"] for r in plain)
        tw = sum(r["wire_bytes"] for r in tail)
        pu = sum(r["upload_ms"] for r in plain)
        tu = sum(r["upload_ms"] for r in tail)
        print(f"合计 {len(trace)} 轮:plain {_mb(pw)} MB / {pu/1000:.2f} s,"
              f"tail {_mb(tw)} MB / {tu/1000:.2f} s({pw/tw:.1f}x,省 {(pu-tu)/1000:.2f} s)")
        if args.mbps:
            print(f"  纯算术:{upload_seconds(pw, args.mbps):.2f} s / "
                  f"{upload_seconds(tw, args.mbps):.2f} s")

        if args.json_out:
            with open(args.json_out, "w") as f:
                json.dump({"sdk": "openai", "rounds": len(trace),
                           "mbps": args.mbps, "plain": plain, "tail": tail,
                           "totals": {"plain_bytes": pw, "tail_bytes": tw,
                                      "ratio": round(pw / tw, 2),
                                      "plain_upload_s": round(pu / 1000, 3),
                                      "tail_upload_s": round(tu / 1000, 3)}},
                          f, ensure_ascii=False, indent=2)
            print(f"已写 {args.json_out}")
    finally:
        for m in (local, cluster):
            if m is not None:
                m.stop()
        if relay is not None:
            relay.stop()
        for s in servers:
            s.stop()
        if not args.keep:
            shutil.rmtree(root, ignore_errors=True)


if __name__ == "__main__":
    main()
