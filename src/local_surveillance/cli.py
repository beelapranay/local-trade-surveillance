"""Small local CLI; no models, agents or external trading operations."""

import argparse
import asyncio
import json
import sys
import time

from .models import dumps
from .pipeline import record, replay, detect_stored
from .store import Store
from .tools import Investigation


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default="data/surveillance.sqlite")
    sub = parser.add_subparsers(dest="command", required=True)
    for name in ("record", "live"):
        p = sub.add_parser(name)
        p.add_argument("--symbols", nargs="+", default=["BTC/USD", "ETH/USD"])
        p.add_argument("--seconds", type=float, default=1200)
        p.add_argument("--depth", type=int, choices=[10, 25, 100, 500, 1000], default=10)
        p.add_argument("--seed", type=int, default=13)
    p = sub.add_parser("replay")
    p.add_argument("--source-run", required=True)
    p.add_argument("--resume-run")
    p.add_argument("--speed", type=float, default=10, help="0 = unpaced")
    p.add_argument("--seed", type=int, default=13)
    p.add_argument("--evaluation-dir", default="eval_data")
    p = sub.add_parser("detect")
    p.add_argument("--run", required=True)
    sub.add_parser("runs")
    p = sub.add_parser("inspect")
    p.add_argument("--run", required=True)
    p.add_argument("--as-of-seq", type=int)
    p.add_argument("--as-of-time", type=float)
    p.add_argument("--tool", choices=["list_alerts", "get_alert", "get_evidence", "get_account", "get_trader_profile",
                                    "get_order_activity", "get_market_context", "get_supporting_records"],
                   default="list_alerts")
    p.add_argument("--args", default="{}", help="JSON keyword arguments")
    p = sub.add_parser("benchmark")
    p.add_argument("--source-run", required=True)
    p.add_argument("--rates", nargs="+", type=int, default=[100, 1000, 10000])
    p.add_argument("--seconds", type=float, default=10)
    p.add_argument("--output", default="benchmark-results.json")
    args = parser.parse_args()
    store = Store(args.db)
    try:
        if args.command in {"record", "live"}:
            if args.seconds <= 0:
                raise ValueError("seconds must be positive")
            result = asyncio.run(record(store, args.symbols, args.seconds, args.depth,
                                        args.command == "live", args.seed))
        elif args.command == "replay":
            result = replay(store, args.source_run, speed=args.speed, seed=args.seed,
                            run_id=args.resume_run, evaluation_dir=args.evaluation_dir)
        elif args.command == "detect":
            result = detect_stored(store, args.run)
        elif args.command == "runs":
            result = store.runs()
        elif args.command == "benchmark":
            from .benchmark import benchmark
            result = benchmark(store, args.source_run, args.rates, args.seconds, args.output)
        else:
            store.run(args.run)
            seq = args.as_of_seq if args.as_of_seq is not None else store.latest_seq(args.run)
            clock = args.as_of_time
            if clock is None:
                row = store.db.execute("SELECT MAX(received_at) FROM events WHERE run_id=? AND seq<=?",
                                       (args.run, seq)).fetchone()
                clock = row[0] if row[0] is not None else time.time()
            tools = Investigation(store, args.run, seq, clock)
            result = getattr(tools, args.tool)(**json.loads(args.args))
        print(dumps(result))
    except KeyboardInterrupt:
        print("Interrupted; committed recording/replay progress is preserved.", file=sys.stderr)
        return 130
    except Exception as exc:
        print(f"{type(exc).__name__}: {exc}", file=sys.stderr)
        return 1
    finally:
        store.close()
    return 0
