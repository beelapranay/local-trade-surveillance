"""Record, replay and run detectors independently of any model service."""

import asyncio
import json
from pathlib import Path
import time
import uuid

from .detect import Detector
from .kraken import BookError, Normalizer
from .models import Event, dumps, stable_id
from .simulate import Simulator, reference_data


class Pipeline:
    def __init__(self, store, run_id, symbols, seed=13, simulate=True, depth=10):
        self.store = store
        self.run_id = run_id
        self.normalizer = Normalizer(depth)
        self.simulator = Simulator(symbols, seed) if simulate else None
        self.detector = Detector(reference_data())
        self.last_tick = float("-inf")
        self.clock = None
        self.connection = None
        self.errors = []

    def install_references(self):
        with self.store.transaction():
            for kind, values in reference_data().items():
                for item_id, value in values.items():
                    self.store.put_reference(self.run_id, kind, item_id, value)

    def _event(self, event, persist):
        if persist:
            seq = self.store.add_event(self.run_id, event)
            if seq is None:
                return
        if self.simulator and event.ts <= self.clock:
            self.simulator.observe(event)
        self.detector.process(event, self.clock)

    def accept(self, row, persist=True):
        self.clock = row["received_at"]
        if self.connection != row["connection_id"]:
            self.normalizer.reset()
            if self.simulator:
                self.simulator.quotes.clear()
            self.connection = row["connection_id"]
        try:
            events = self.normalizer.normalize(row["raw"], self.clock, self.connection, row["seq"])
        except BookError as exc:
            self.errors.append({"raw_seq": row["seq"], "reason": str(exc)})
            self.normalizer.reset()
            events = [Event(stable_id("bad-book", row["seq"]), self.clock, self.clock, "gap", None,
                            "system", {"reason": str(exc), "raw_seq": row["seq"]})]
        self.accept_events(events, self.clock, persist)
        if persist:
            self.store.set_cursor(self.run_id, row["seq"])
        return bool(events and events[0].kind == "gap")

    def accept_events(self, events, clock, persist=True):
        self.clock = clock
        for event in events:
            self._event(event, persist)
        if self.simulator:
            for event in self.simulator.step(clock):
                self._event(event, persist)
        self.tick(clock, persist=persist)

    def tick(self, clock, persist=True, force=False):
        if not force and clock - self.last_tick < 5:
            return
        self.last_tick = clock
        if persist:
            # A new sequence captures alert maturation even when the feed only sends heartbeats.
            self.store.add_event(self.run_id, Event(stable_id("clock", clock), clock, clock,
                                                   "clock", None, "system", {}))
        for alert in self.detector.advance(clock):
            if persist:
                self.store.save_alert(self.run_id, alert, self.store.latest_seq(self.run_id), clock)

    def report(self):
        return {"run_id": self.run_id, "alerts": len(self.detector.alerts),
                "rule_counts": {rule: sum(a["rule"] == rule for a in self.detector.alerts.values())
                                for rule in sorted({a["rule"] for a in self.detector.alerts.values()})},
                "pending_layering_fills": len(self.detector.pending_fills),
                "pending_cancel_buckets": len(self.detector.cancel_buckets),
                "book_errors": self.errors, "order_errors": self.detector.errors,
                "simulation_paused_steps": self.simulator.paused_steps if self.simulator else 0,
                "scenarios_remaining": len(self.simulator.schedule) - self.simulator.next_step
                                       if self.simulator else 0}


async def record(store, symbols, seconds=1200, depth=10, simulate=False, seed=13, max_reconnects=8):
    from websockets.asyncio.client import connect
    from websockets.exceptions import ConnectionClosed

    config = {"symbols": symbols, "depth": depth, "simulate": simulate, "seed": seed}
    run_id = store.create_run("live" if simulate else "record", config)
    pipeline = Pipeline(store, run_id, symbols, seed, simulate, depth)
    pipeline.install_references()
    print(dumps({"run_id": run_id, "status": "connecting", "symbols": symbols}), flush=True)
    deadline = time.monotonic() + seconds
    reconnects = 0
    try:
        while time.monotonic() < deadline:
            connection_id = uuid.uuid4().hex
            try:
                async with connect("wss://ws.kraken.com/v2", open_timeout=15, close_timeout=5,
                                   max_queue=1024, ping_interval=20) as ws:
                    for channel in ("book", "trade"):
                        params = {"channel": channel, "symbol": symbols, "snapshot": channel == "book"}
                        if channel == "book":
                            params["depth"] = depth
                        await ws.send(dumps({"method": "subscribe", "params": params}))
                    while time.monotonic() < deadline:
                        raw = await asyncio.wait_for(ws.recv(), timeout=min(30, max(.01, deadline - time.monotonic())))
                        clock = time.time()
                        with store.transaction():
                            seq = store.save_raw(run_id, clock, connection_id, raw)
                            gap = pipeline.accept({"raw": raw, "received_at": clock,
                                                   "connection_id": connection_id, "seq": seq})
                        if gap:
                            raise BookError("Reconnecting for a fresh validated snapshot")
            except (OSError, ConnectionClosed, TimeoutError, BookError) as exc:
                if time.monotonic() >= deadline:
                    break
                reconnects += 1
                clock = time.time()
                with store.transaction():
                    raw = dumps({"type": "local_gap", "reason": str(exc)})
                    seq = store.save_raw(run_id, clock, connection_id, raw)
                    pipeline.accept({"raw": raw, "received_at": clock,
                                     "connection_id": connection_id, "seq": seq})
                if reconnects > max_reconnects:
                    raise RuntimeError("Reconnect limit exceeded") from exc
                print(dumps({"run_id": run_id, "status": "reconnecting", "reason": str(exc)}), flush=True)
                await asyncio.sleep(min(2 ** (reconnects - 1), 10, max(0, deadline - time.monotonic())))
        if pipeline.clock is not None:
            with store.transaction():
                pipeline.tick(pipeline.clock, force=True)
        count = store.db.execute("SELECT COUNT(*) FROM raw_messages WHERE run_id=?", (run_id,)).fetchone()[0]
        if not count or not pipeline.normalizer.books:
            raise RuntimeError("No validated order-book data recorded")
        store.set_status(run_id, "complete")
    except (KeyboardInterrupt, asyncio.CancelledError):
        store.set_status(run_id, "interrupted")
        raise
    except Exception as exc:
        store.set_status(run_id, "failed", str(exc))
        raise
    return pipeline.report()


def replay(store, source_run, *, speed=10, seed=13, run_id=None, evaluation_dir=None,
           max_messages=None):
    """Resume by rebuilding deterministic state through the transactional raw cursor.

    speed=0 means unpaced replay. max_messages is useful for interrupt/recovery tests.
    Only new suffix messages mutate the resumed run.
    """
    source = store.run(source_run)
    if source["mode"] not in {"record", "live"}:
        raise ValueError("Replay source must contain a live recording")
    if speed < 0:
        raise ValueError("speed must be non-negative")
    config = {"source_run": source_run, "symbols": source["config"]["symbols"],
              "depth": source["config"]["depth"], "seed": seed, "simulate": True}
    if run_id is None:
        run_id = store.create_run("replay", config)
    target = store.run(run_id)
    if target["mode"] != "replay" or target["config"] != config:
        raise ValueError("Resume configuration must match the original replay")
    store.set_status(run_id, "running")
    pipeline = Pipeline(store, run_id, config["symbols"], seed, True, config["depth"])
    if target["cursor"] == 0:
        pipeline.install_references()
    previous_clock = None
    processed = 0
    interrupted = False
    try:
        for row in store.raw(source_run):
            rebuilding = row["seq"] <= target["cursor"]
            if not rebuilding:
                if max_messages is not None and processed >= max_messages:
                    interrupted = True
                    break
                if speed and previous_clock is not None:
                    delay = max(0, row["received_at"] - previous_clock) / speed
                    # Break long waits into bounded sleeps so Ctrl-C remains responsive.
                    deadline = time.monotonic() + delay
                    while time.monotonic() < deadline:
                        time.sleep(min(.2, deadline - time.monotonic()))
                with store.transaction():
                    pipeline.accept(row)
                processed += 1
            else:
                pipeline.accept(row, persist=False)
            previous_clock = row["received_at"]
        if pipeline.clock is None:
            raise ValueError("Source recording is empty")
        if not interrupted:
            with store.transaction():
                pipeline.tick(pipeline.clock, force=True)
        store.set_status(run_id, "interrupted" if interrupted else "complete")
    except KeyboardInterrupt:
        store.set_status(run_id, "interrupted")
        raise
    except Exception as exc:
        store.set_status(run_id, "failed", str(exc))
        raise
    if evaluation_dir and pipeline.simulator.start is not None:
        root = Path(evaluation_dir)
        root.mkdir(parents=True, exist_ok=True)
        (root / f"{run_id}.json").write_text(dumps(pipeline.simulator.evaluation()) + "\n")
    return pipeline.report()


def detect_stored(store, run_id):
    """Recompute rules from the run's persisted events in availability order."""
    detector = Detector(store.references(run_id))
    rows = store.db.execute("SELECT * FROM events WHERE run_id=? ORDER BY seq", (run_id,))
    for row in rows:
        event = Event(row["event_id"], row["ts"], row["received_at"], row["kind"], row["symbol"],
                      row["source"], json.loads(row["payload"]))
        if event.kind == "clock":
            for alert in detector.advance(event.ts):
                store.save_alert(run_id, alert, row["seq"], event.ts)
        else:
            detector.process(event)
    store.db.commit()
    return {"run_id": run_id, "alerts": len(detector.alerts), "order_errors": detector.errors}
