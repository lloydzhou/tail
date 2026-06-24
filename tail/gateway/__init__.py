"""Tail Python Gateway —— FastAPI 版网关(对应 OpenResty gateway.lua)。

用法(命令行):
    python -m tail.gateway --backend https://api.deepseek.com --port 8765

用法(代码):
    from tail.gateway import build_app, DbmStorage, GatewayConfig
    cfg = GatewayConfig(backend_url="https://api.deepseek.com")
    storage = DbmStorage(cfg, "./tail_cache.dbm")
    app = build_app(cfg, storage)
    # 然后 uvicorn.run(app, port=8765)
"""

from __future__ import annotations

import argparse
import logging
import os
import sys

from .app import build_app
from .protocol import GatewayConfig
from .storage import DbmStorage, Storage

__all__ = ["build_app", "DbmStorage", "Storage", "GatewayConfig", "main"]


def _make_storage(cfg: GatewayConfig, args) -> Storage:
    """根据 --storage 参数选择存储后端。默认 dbm(零依赖)。"""
    if args.storage == "dbm":
        return DbmStorage(cfg, args.dbm_path)
    if args.storage == "redis":
        try:
            from .redis_storage import RedisStorage
        except ImportError:
            print("RedisStorage 需要 redis-py: pip install redis", file=sys.stderr)
            sys.exit(1)
        return RedisStorage(cfg, host=args.kvrocks_host, port=args.kvrocks_port)
    raise ValueError(f"unknown storage: {args.storage}")


def main(argv=None) -> None:
    """命令行入口:python -m tail.gateway --backend URL [options]。"""
    parser = argparse.ArgumentParser(
        prog="python -m tail.gateway",
        description="Tail Python Gateway — prefix-cache negotiation gateway for OpenAI Chat Completions",
    )
    parser.add_argument("--backend", required=True,
                        help="后端推理服务 base URL,如 https://api.deepseek.com")
    parser.add_argument("--port", type=int, default=8765, help="网关监听端口(默认 8765)")
    parser.add_argument("--host", default="127.0.0.1", help="监听地址(默认 127.0.0.1)")
    parser.add_argument("--storage", default="dbm", choices=["dbm", "redis"],
                        help="存储后端(默认 dbm,零依赖;redis 连 Kvrocks)")
    parser.add_argument("--dbm-path", default="./tail_cache.dbm",
                        help="dbm 文件路径(storage=dbm 时)")
    parser.add_argument("--kvrocks-host", default="127.0.0.1")
    parser.add_argument("--kvrocks-port", type=int, default=6666)
    parser.add_argument("--miss-mode", default="fast_fail",
                        choices=["fast_fail", "passthrough"])
    parser.add_argument("--debug", action="store_true",
                        help="开启详细 debug 日志(缓存命中/未命中/存储内容)")
    parser.add_argument("--log-level", default=None,
                        help="日志级别(默认 debug 开时为 DEBUG,否则 INFO)")
    args = parser.parse_args(argv)

    log_level = args.log_level or ("DEBUG" if args.debug else "INFO")
    logging.basicConfig(level=log_level.upper(),
                        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

    cfg = GatewayConfig(
        backend_url=args.backend,
        miss_mode=args.miss_mode,
        hash_ns=os.environ.get("TAIL_HASH_NS", "prefix_cache"),
        debug=args.debug,
    )
    storage = _make_storage(cfg, args)
    app = build_app(cfg, storage)

    import uvicorn
    uvicorn.run(app, host=args.host, port=args.port, log_level=args.log_level)


if __name__ == "__main__":
    main()
