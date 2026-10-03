"""Measured, bounded stress replay. Repetition is explicit, never new real trades."""

from dataclasses import replace
import itertools
from pathlib import Path
import time

from .kraken import BookError, Normalizer
from .models import dumps, stable_id
from .pipeline import Pipeline


def benchmark(store, source_run, rates=(100, 1000, 10000), seconds=10, output=None):
    if seconds <= 0 or any(rate <= 0 for rate in rates):
        raise ValueError("Rates and duration must be positive")
    source = store.run(source_run)
    config = source["config"]
    if source["mode"] not in {"record", "live"}:
        raise ValueError("Benchmark requires a live recording")
    # Bound sample RAM; every repetition restarts from a validated snapshot.
    rows = list(itertools.islice(store.raw(source_run), 50000))
    if not rows or not any('"book"' in row["raw"] for row in rows):
        raise ValueError("Recording has no book data")
    probe = Normalizer(config["depth"])
    valid = False
    connection = None
    for row in rows:
        if connection != row["connection_id"]:
            connection = row["connection_id"]; probe.reset()
        try:
            valid |= any(e.kind == "market_bbo" for e in probe.normalize(
                row["raw"], row["received_at"], connection, row["seq"]))
        except BookError:
            probe.reset()
    if not valid:
        raise ValueError("Recording has no validated order-book snapshot")
    outcomes = []
    for rate in rates:
        run_id = store.create_run("benchmark", {"source_run": source_run, "target_rate": rate,
                                                "seconds": seconds, "simulated_speed": 100})
        pipeline = Pipeline(store, run_id, config["symbols"], depth=config["depth"])
        pipeline.install_references()
        count = raw_count = cycles = 0
        index = 0
        normalizer = Normalizer(config["depth"])
        connection = None
        budget = int(rate * seconds)
        begun = time.perf_counter()
        processing_seconds = 0
        while count < budget:
            batch_start = time.perf_counter()
            batch_count = 0
            with store.transaction():
                while batch_count < 200 and count < budget:
                    if index == len(rows):
                        index = 0; cycles += 1; normalizer.reset(); connection = None
                    row = rows[index]; index += 1
                    if connection != row["connection_id"]:
                        connection = row["connection_id"]; normalizer.reset()
                    try:
                        events = normalizer.normalize(row["raw"], row["received_at"], connection, row["seq"])
                    except BookError:
                        normalizer.reset(); continue
                    raw_count += 1
                    if not events:
                        continue
                    events = events[:budget - count]
                    clock = rows[0]["received_at"] + count / rate * 100
                    # Map source timestamps onto the benchmark's 100x simulation clock.
                    mapped = [replace(e, event_id=stable_id("benchmark", count, i, e.event_id),
                                      ts=min(clock, clock + e.ts - row["received_at"]), received_at=clock)
                              for i, e in enumerate(events)]
                    seq = store.save_raw(run_id, clock, f"benchmark-{cycles}", row["raw"])
                    pipeline.accept_events(mapped, clock)
                    store.set_cursor(run_id, seq)
                    count += len(mapped); batch_count += len(mapped)
            processing_seconds += time.perf_counter() - batch_start
            # Pace the measured delivery in 200-event batches; backlog never grows unbounded.
            delivery_deadline = begun + count / rate
            while time.perf_counter() < delivery_deadline:
                time.sleep(min(delivery_deadline - time.perf_counter(), .2))
        elapsed = time.perf_counter() - begun
        with store.transaction():
            pipeline.tick(pipeline.clock, force=True)
        store.set_status(run_id, "complete")
        total = store.db.execute("SELECT COUNT(*) FROM events WHERE run_id=?", (run_id,)).fetchone()[0]
        outcomes.append({"run_id": run_id, "target_market_events_per_sec": rate,
                         "delivered_market_events": count, "persisted_events_including_simulation": total,
                         "raw_messages_processed": raw_count, "recording_repetitions": cycles + 1,
                         "elapsed_seconds": elapsed, "processing_seconds_excluding_pacing": processing_seconds,
                         "achieved_market_events_per_sec": count / elapsed,
                         "processing_capacity_events_per_sec": count / processing_seconds,
                         "kept_up": elapsed <= seconds * 1.1, "pipeline": pipeline.report()})
    report = {"source_run": source_run, "input": "repeated real recording, explicitly retimed",
              "simulation_speed": 100, "transaction_batch_market_events": 200,
              "includes": ["JSON parsing", "book checksum validation", "raw and normalized SQLite writes",
                           "internal simulation", "rule evaluation"],
              "limitation": "Finite local stress replay; excludes network and model investigation throughput.",
              "results": outcomes}
    if output:
        Path(output).write_text(dumps(report) + "\n")
    return report
