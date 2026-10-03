import unittest

from local_surveillance.detect import Detector, detect
from local_surveillance.models import Event
from local_surveillance.simulate import reference_data
from helpers import order


class DetectorTests(unittest.TestCase):
    def test_layering_waits_for_cancellation_window_and_merges(self):
        d = Detector(reference_data())
        for i in range(5):
            d.process(order(str(i), "NEW", i, qty=10))
        d.process(order("exec", "NEW", 7, side="SELL", qty=5))
        d.process(order("exec", "FILL", 8, side="SELL", qty=5))
        self.assertFalse(d.advance(9))
        for i in range(5):
            d.process(order(str(i), "CANCEL", 15 + i, qty=10))
        self.assertFalse(d.advance(37))
        alerts = d.advance(38)
        self.assertEqual(len(alerts), 1)
        self.assertEqual(alerts[0]["rule"], "LAYERING")
        self.assertEqual(alerts[0]["metrics"]["cancel_fraction"], 1)
        self.assertFalse(alerts[0]["market_impact_proven"])
        self.assertFalse(d.advance(39))

    def test_cancels_before_execution_do_not_count_as_layering(self):
        events = [order(str(i), "NEW", i, qty=10) for i in range(5)]
        events += [order(str(i), "CANCEL", 6, qty=10) for i in range(5)]
        events += [order("exec", "NEW", 7, side="SELL", qty=5),
                   order("exec", "FILL", 8, side="SELL", qty=5)]
        self.assertFalse(detect(events, reference_data(), 40))

    def test_unknown_ownership_never_creates_wash_alert(self):
        events = [order("w", "NEW", 1, account="UNKNOWN-A", side="SELL", contra="UNKNOWN-B"),
                  order("w", "FILL", 2, account="UNKNOWN-A", side="SELL", contra="UNKNOWN-B")]
        self.assertFalse(detect(events, reference_data(), 40))

    def test_wash_merges_crosses(self):
        d = Detector(reference_data())
        for i in range(3):
            d.process(order(f"w{i}", "NEW", i * 4, trader="T05", account="LIQ-A", side="SELL", contra="LIQ-B"))
            d.process(order(f"w{i}", "FILL", i * 4 + 1, trader="T05", account="LIQ-A", side="SELL", contra="LIQ-B"))
        result = d.advance(20)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["n_triggers"], 3)
        self.assertEqual(len(result[0]["evidence_event_ids"]), 6)

    def test_partial_fills_dedupe_and_cancel_count_orders(self):
        d = Detector(reference_data())
        for i in range(45):
            d.process(order(str(i), "NEW", i, trader="T02", account="HEDGE", qty=2))
        fill = order("0", "FILL", 46, trader="T02", account="HEDGE", qty=.5)
        d.process(fill); d.process(fill)
        d.process(order("0", "FILL", 47, trader="T02", account="HEDGE", qty=.5))
        self.assertEqual(d.orders["0"].filled, 1)
        for i in range(45):
            d.process(order(str(i), "CANCEL", 50 + i, trader="T02", account="HEDGE", qty=2))
        self.assertFalse(d.advance(899))
        alerts = d.advance(900)
        self.assertEqual(alerts[0]["metrics"]["filled_orders"], 1)
        self.assertEqual(alerts[0]["metrics"]["cancelled_orders"], 45)

    def test_overfill_and_fill_after_cancel_rejected(self):
        d = Detector(reference_data())
        d.process(order("a", "NEW", 0))
        d.process(order("a", "FILL", 1, qty=2))
        d.process(order("a", "CANCEL", 2))
        d.process(order("a", "FILL", 3))
        self.assertEqual(len(d.errors), 2)
        self.assertEqual(d.orders["a"].filled, 0)

    def test_front_running_stops_after_client_completion(self):
        d = Detector(reference_data())
        d.process(Event("client", 1, 1, "client_order", "ETH/USD", "synthetic",
                        {"event": "RECEIVED", "client_order_id": "c", "account": "CLIENT-A",
                         "side": "BUY", "qty": 50, "handling_trader": "T03"}))
        d.process(order("f", "NEW", 2, trader="T03", account="PERSONAL"))
        d.process(order("f", "FILL", 3, trader="T03", account="PERSONAL"))
        d.process(Event("done", 4, 4, "client_order", "ETH/USD", "synthetic",
                        {"event": "COMPLETED", "client_order_id": "c"}))
        d.process(order("later", "NEW", 5, trader="T03", account="PERSONAL"))
        d.process(order("later", "FILL", 6, trader="T03", account="PERSONAL"))
        alert = d.advance(10)[0]
        self.assertEqual(alert["n_triggers"], 1)

    def test_future_event_invisible(self):
        events = [order("w", "NEW", 1, trader="T05", account="LIQ-A", side="SELL", contra="LIQ-B")]
        future = order("w", "FILL", 100, trader="T05", account="LIQ-A", side="SELL", contra="LIQ-B")
        events.append(Event(future.event_id, future.ts, 2, future.kind, future.symbol, future.source, future.payload))
        self.assertFalse(detect(events, reference_data(), 50))
        self.assertEqual(detect(events, reference_data(), 100)[0]["rule"], "WASH_TRADE")

    def test_long_lived_active_order_survives_state_pruning(self):
        d = Detector(reference_data())
        d.process(order("long", "NEW", 0))
        d.advance(4000)
        d.process(order("long", "CANCEL", 4001))
        self.assertFalse(d.errors)
        self.assertEqual(d.orders["long"].cancelled_at, 4001)
