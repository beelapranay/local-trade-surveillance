import json
from pathlib import Path
import tempfile
import unittest

from local_surveillance.models import Event
from local_surveillance.pipeline import Pipeline, replay, detect_stored
from local_surveillance.simulate import Simulator
from local_surveillance.store import Store
from local_surveillance.tools import Investigation
from helpers import book_message, recording, order


class PipelineTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.store = Store(Path(self.tmp.name) / "test.sqlite")

    def tearDown(self):
        self.store.close(); self.tmp.cleanup()

    def test_full_scenarios_and_evidence(self):
        source = recording(self.store)
        report = replay(self.store, source, speed=0, evaluation_dir=Path(self.tmp.name) / "eval")
        run = report["run_id"]
        self.assertFalse(report["order_errors"])
        self.assertEqual(report["scenarios_remaining"], 0)
        self.assertEqual(report["rule_counts"], {"EXCESSIVE_CANCELS": 2, "FRONT_RUNNING": 2,
                                                "LAYERING": 2, "WASH_TRADE": 2})
        tools = Investigation(self.store, run, self.store.latest_seq(run), 1000)
        alerts = tools.list_alerts()
        self.assertEqual(len(alerts), 8)
        visible = {e["event_id"] for e in self.store.events(run, as_of_seq=tools.as_of_seq, as_of_time=1000)}
        for alert in alerts:
            self.assertTrue(set(alert["evidence_event_ids"]) <= visible)
            self.assertFalse(tools.get_evidence(alert["evidence_event_ids"])["unavailable_event_ids"])
        records = tools.get_supporting_records(0, 300)["events"]
        self.assertTrue({"client_instruction", "options_position", "algo", "approval", "chat"}
                        <= {r["kind"] for r in records})
        hedge = tools.get_supporting_records(0, 100, trader_id="T02")["events"]
        position = next(r for r in hedge if r["kind"] == "options_position")
        self.assertEqual(float(position["payload"]["delta_units"]), -5)
        fills = tools.get_order_activity(50, 80, trader_id="T02")["events"]
        fills = [r for r in fills if r["payload"]["event"] == "FILL"]
        self.assertEqual(len(fills), 1)
        self.assertEqual((fills[0]["payload"]["side"], float(fills[0]["payload"]["qty"])), ("BUY", 5))
        execution = tools.get_supporting_records(0, 300, trader_id="T04")["events"]
        algo = next(r for r in execution if r["kind"] == "algo")
        client = next(r for r in execution if r["kind"] == "client_order")
        self.assertLess(algo["ts"], client["ts"])
        self.assertTrue(algo["payload"]["approved"])
        self.assertTrue((Path(self.tmp.name) / "eval" / f"{run}.json").exists())
        recomputed = detect_stored(self.store, run)
        self.assertEqual(recomputed["alerts"], len(alerts))

    def test_interrupted_resume_matches_fresh_replay(self):
        source = recording(self.store)
        interrupted = replay(self.store, source, speed=0, max_messages=370)
        run = interrupted["run_id"]
        self.assertEqual(self.store.run(run)["status"], "interrupted")
        resumed = replay(self.store, source, speed=0, run_id=run)
        fresh = replay(self.store, source, speed=0)
        self.assertEqual(resumed["rule_counts"], fresh["rule_counts"])
        def alerts(r):
            return self.store.alerts(r, as_of_seq=self.store.latest_seq(r), as_of_time=1000)
        self.assertEqual(alerts(run), alerts(fresh["run_id"]))
        def count(r):
            return self.store.db.execute("SELECT COUNT(*) FROM events WHERE run_id=?", (r,)).fetchone()[0]
        self.assertEqual(count(run), count(fresh["run_id"]))
        again = replay(self.store, source, speed=0, run_id=run)
        self.assertEqual(again["rule_counts"], fresh["rule_counts"])
        self.assertEqual(count(run), count(fresh["run_id"]))

    def test_future_and_later_ingestion_hidden_from_tools(self):
        run = self.store.create_run("test", {})
        a = order("a", "NEW", 1)
        with self.store.transaction():
            seq = self.store.add_event(run, a)
            self.store.add_event(run, order("b", "NEW", 100))
            self.store.add_event(run, order("c", "NEW", 2))
        tools = Investigation(self.store, run, seq, 50)
        records = tools.get_order_activity(0, 1000)
        self.assertEqual(records["events_total"], 1)
        self.assertEqual(records["events"][0]["event_id"], a.event_id)
        evidence = tools.get_evidence([a.event_id, "b-NEW-100"])
        self.assertEqual(evidence["unavailable_event_ids"], ["b-NEW-100"])
        tools2 = Investigation(self.store, run, self.store.latest_seq(run), 50)
        self.assertEqual(tools2.get_order_activity(0, 1000)["events_total"], 2)

    def test_gap_pauses_and_requires_fresh_snapshot(self):
        run = self.store.create_run("live", {"symbols": ["ETH/USD"]})
        p = Pipeline(self.store, run, ["ETH/USD"])
        p.install_references()
        def send(raw, clock, seq):
            with self.store.transaction():
                return p.accept({"raw": raw, "received_at": clock, "connection_id": "c", "seq": seq})
        send(book_message(), 0, 1)
        send('{"type":"local_gap","reason":"network"}', 10, 2)
        before = p.simulator.counter
        send('{"channel":"heartbeat"}', 30, 3)
        self.assertEqual(p.simulator.counter, before)
        self.assertGreater(p.simulator.paused_steps, 0)
        send(book_message(), 40, 4)
        self.assertGreater(p.simulator.counter, before)

    def test_deduplicated_event_insert(self):
        run = self.store.create_run("test", {})
        e = order("a", "NEW", 1)
        with self.store.transaction():
            self.assertIsNotNone(self.store.add_event(run, e))
            self.assertIsNone(self.store.add_event(run, e))

    def test_run_start_references_are_immutable(self):
        run = self.store.create_run("test", {})
        with self.store.transaction():
            self.store.put_reference(run, "accounts", "a", {"controlled_by": "owner"})
            self.store.add_event(run, order("o", "NEW", 1))
            with self.assertRaisesRegex(ValueError, "immutable"):
                self.store.put_reference(run, "accounts", "a", {"controlled_by": "someone else"})
            with self.assertRaisesRegex(ValueError, "before ingestion"):
                self.store.put_reference(run, "accounts", "b", {"controlled_by": "owner"})

    def test_alert_snapshot_preserves_old_version(self):
        run = self.store.create_run("test", {})
        with self.store.transaction():
            self.store.save_alert(run, {"alert_id": "a", "rule": "WASH_TRADE", "n_triggers": 1}, 1, 1)
            self.store.save_alert(run, {"alert_id": "a", "rule": "WASH_TRADE", "n_triggers": 2}, 2, 2)
        self.assertEqual(Investigation(self.store, run, 1, 10).get_alert("a")["n_triggers"], 1)
        self.assertEqual(Investigation(self.store, run, 2, 1).get_alert("a")["n_triggers"], 1)

    def test_simulator_never_uses_future_quote(self):
        run = self.store.create_run("live", {"symbols": ["ETH/USD"]})
        p = Pipeline(self.store, run, ["ETH/USD"])
        future = Event("future-bbo", 100, 1, "market_bbo", "ETH/USD", "kraken",
                       {"bid": "100", "ask": "101", "bid_size": "1", "ask_size": "1"})
        with self.store.transaction():
            p.accept_events([future], 1)
        self.assertIsNone(p.simulator.start)
        self.assertFalse(p.simulator.quotes)

    def test_benchmark_pacing_and_invalid_input(self):
        from local_surveillance.benchmark import benchmark
        source = recording(self.store, seconds=2)
        result = benchmark(self.store, source, rates=[1000], seconds=.02)["results"][0]
        self.assertEqual(result["delivered_market_events"], 20)
        self.assertGreaterEqual(result["elapsed_seconds"], .018)
        empty = self.store.create_run("record", {"symbols": ["ETH/USD"], "depth": 10})
        with self.store.transaction():
            self.store.save_raw(empty, 1, "c", '{"channel":"book","data":[]}')
        with self.assertRaisesRegex(ValueError, "validated"):
            benchmark(self.store, empty, [100], .01)
