"""Deterministic internal scenarios against external prices, never fake attribution.

Only fictional order activity is generated. Nothing is sent to an exchange.
The schedule/answer key is private evaluation data, not investigation evidence.
"""

import math
import random
from collections import defaultdict

from .models import Event, stable_id


def reference_data():
    traders = {
        "T01": {"trader_id": "T01", "desk": "Prop", "role": "proprietary"},
        "T02": {"trader_id": "T02", "desk": "Options", "role": "market_maker"},
        "T03": {"trader_id": "T03", "desk": "Execution A", "role": "execution"},
        "T04": {"trader_id": "T04", "desk": "Execution B", "role": "execution"},
        "T05": {"trader_id": "T05", "desk": "Liquidity", "role": "liquidity_provider"},
        "T06": {"trader_id": "T06", "desk": "Crossing", "role": "execution"},
    }
    definitions = [
        ("PROP", "prop", "T01", "Fictional Firm"),
        ("HEDGE", "market_making", "T02", "Fictional Firm"),
        ("PERSONAL", "employee_personal", "T03", "T03"),
        ("ALGO", "facilitation", "T04", "Fictional Firm"),
        ("LIQ-A", "liquidity", "T05", "Fictional Firm"),
        ("LIQ-B", "client_dma", "T05", "Fictional Affiliate"),
        ("FUND-A", "client", "Fictional Manager", "Fictional Fund A"),
        ("FUND-B", "client", "Fictional Manager", "Fictional Fund B"),
        ("CLIENT-A", "client", "Client A", "Client A"),
        ("CLIENT-B", "client", "Client B", "Client B"),
    ]
    accounts = {a: {"account_id": a, "type": t, "controlled_by": c,
                    "beneficial_owner": owner, "identity_source": "synthetic"}
                for a, t, c, owner in definitions}
    return {"traders": traders, "accounts": accounts}


class Simulator:
    def __init__(self, symbols, seed=13, freshness=30):
        self.symbols = list(symbols)
        self.seed = seed
        self.random = random.Random(seed)
        self.freshness = freshness
        self.quotes = {}
        self.start = None
        self.schedule = []
        self.next_step = 0
        self.counter = 0
        self.order_prices = {}
        self.truth = []
        self.background = defaultdict(lambda: float("-inf"))
        self.paused_steps = 0

    def observe(self, event):
        if event.kind == "gap":
            self.quotes.clear()
        elif event.kind == "market_bbo":
            self.quotes[event.symbol] = (event.received_at, event.payload)
            if self.start is None and all(s in self.quotes for s in self.symbols):
                self.start = event.received_at
                self._plan()

    def _plan(self):
        symbol = self.symbols[-1]
        base = self.start

        def support(offset, kind, value):
            self.schedule.append((base + offset, kind, symbol, value))

        def order(offset, trader, account, oid, side, qty, action="NEW", contra="EXT", parent=None):
            self.schedule.append((base + offset, "order", symbol,
                {"order_id": "O-" + stable_id(self.seed, oid), "trader_id": trader, "account": account, "side": side,
                 "qty": str(qty), "event": action, "contra_account": contra,
                 "parent_id": parent, "identity_source": "synthetic"}))

        def layering(offset, trader, account, prefix, hedge=False):
            for i in range(5):
                order(offset + i, trader, account, f"{prefix}-{i}", "SELL" if hedge else "BUY", 10)
                order(offset + 15 + i, trader, account, f"{prefix}-{i}", "SELL" if hedge else "BUY", 10, "CANCEL")
            order(offset + 7, trader, account, f"{prefix}-fill", "BUY" if hedge else "SELL", 5)
            order(offset + 8, trader, account, f"{prefix}-fill", "BUY" if hedge else "SELL", 5, "FILL")
            self.truth.append({"rule": "LAYERING", "trader_id": trader, "symbol": symbol,
                               "start": base + offset, "end": base + offset + 19,
                               "should_escalate": not hedge})

        layering(15, "T01", "PROP", "O-P")
        support(45, "options_position", {"record_id": "OPT-1", "trader_id": "T02",
            "underlying": symbol, "delta_units": "-5", "reason": "Client call sale; buy five units to hedge"})
        layering(50, "T02", "HEDGE", "O-H", hedge=True)
        support(49, "chat", {"record_id": "CHAT-H", "participant": "T02",
                             "text": "Bought five units to offset the short five-unit options delta."})

        for i in range(3):
            order(100 + i * 4, "T05", "LIQ-A", f"O-W-{i}", "SELL", 3, contra="LIQ-B")
            order(101 + i * 4, "T05", "LIQ-A", f"O-W-{i}", "SELL", 3, "FILL", "LIQ-B")
        self.truth.append({"rule": "WASH_TRADE", "trader_id": "T05", "symbol": symbol,
                           "start": base + 101, "end": base + 109, "should_escalate": True})
        support(125, "client_instruction", {"record_id": "CI-1", "handling_trader": "T06",
            "accounts": ["FUND-A", "FUND-B"], "qty": "3", "instruction": "Approved fund cross at mid"})
        order(130, "T06", "FUND-A", "O-CROSS", "SELL", 3, contra="FUND-B")
        order(131, "T06", "FUND-A", "O-CROSS", "SELL", 3, "FILL", "FUND-B")
        self.truth.append({"rule": "WASH_TRADE", "trader_id": "T06", "symbol": symbol,
                           "start": base + 131, "end": base + 131, "should_escalate": False})

        for offset, trader, client, account, oid in (
                (160, "T03", "CLIENT-A", "PERSONAL", "CL-1"),
                (240, "T04", "CLIENT-B", "ALGO", "CL-2")):
            support(offset, "client_order", {"client_order_id": oid, "event": "RECEIVED",
                "account": client, "side": "BUY", "qty": "50", "handling_trader": trader})
            order(offset + 10, trader, account, f"O-{oid}", "BUY", 2,
                  parent="TWAP-1" if trader == "T04" else None)
            order(offset + 11, trader, account, f"O-{oid}", "BUY", 2, "FILL",
                  parent="TWAP-1" if trader == "T04" else None)
            support(offset + 55, "client_order", {"client_order_id": oid, "event": "COMPLETED"})
            self.truth.append({"rule": "FRONT_RUNNING", "trader_id": trader, "symbol": symbol,
                               "start": base + offset, "end": base + offset + 11,
                               "should_escalate": trader == "T03"})
        support(225, "algo", {"record_id": "TWAP-1", "trader_id": "T04", "account": "ALGO",
            "side": "BUY", "qty": "2", "approved": True, "created_at": base + 225,
            "schedule": {"start": base + 250, "end": base + 251}})
        support(226, "approval", {"record_id": "APP-1", "trader_id": "T04",
                                   "algo_id": "TWAP-1", "approved": True})

        # Fit cancellations within one UTC-aligned 15-minute bucket.
        excess = 320.0
        if (base + excess) % 900 > 850:
            excess += 900 - (base + excess) % 900 + 5
        for i in range(45):
            order(excess + i * .5, "T02", "HEDGE", f"O-X-{i}", "BUY", .2)
            order(excess + i * .5 + .1, "T02", "HEDGE", f"O-X-{i}", "BUY", .2, "CANCEL")
        self.truth.append({"rule": "EXCESSIVE_CANCELS", "trader_id": "T02", "symbol": symbol,
                           "start": math.floor((base + excess) / 900) * 900,
                           "end": (math.floor((base + excess) / 900) + 1) * 900,
                           "should_escalate": False})
        self.schedule.sort(key=lambda row: row[0])

    def _event(self, ts, clock, kind, symbol, value):
        value = dict(value)
        if kind == "order":
            quote = self.quotes[symbol][1]
            oid = value["order_id"]
            if value["event"] == "NEW":
                price = (quote["bid"] if value["side"] == "BUY" else quote["ask"])
                self.order_prices[oid] = price
            price = self.order_prices[oid]
            if value["event"] == "FILL":
                price = quote["ask"] if value["side"] == "BUY" else quote["bid"]
                if value["contra_account"] != "EXT":
                    price = str((float(quote["bid"]) + float(quote["ask"])) / 2)
            value.update({"price": price, "order_type": "LIMIT"})
        self.counter += 1
        return Event(stable_id("simulation", self.seed, self.counter), ts, clock,
                     kind, symbol, "synthetic", value)

    def step(self, clock):
        if self.start is None:
            return []
        # Both sides must be recently validated before creating fictional activity.
        if any(s not in self.quotes or clock - self.quotes[s][0] > self.freshness for s in self.symbols):
            self.paused_steps += 1
            return []
        result = []
        while self.next_step < len(self.schedule) and self.schedule[self.next_step][0] <= clock:
            _, kind, symbol, value = self.schedule[self.next_step]
            # Availability is now, even when gaps delayed the scenario. Do not backdate evidence.
            result.append(self._event(clock, clock, kind, symbol, value))
            self.next_step += 1
        for symbol in self.symbols:
            if clock - self.background[symbol] >= 20:
                self.background[symbol] = clock
                oid = f"O-BG-{symbol}-{self.counter}"
                side = self.random.choice(["BUY", "SELL"])
                qty = str(round(self.random.uniform(.1, .5), 6))
                value = {"order_id": oid, "trader_id": "T02", "account": "HEDGE",
                         "side": side, "qty": qty, "contra_account": "EXT", "parent_id": None,
                         "identity_source": "synthetic", "event": "NEW"}
                result.append(self._event(clock, clock, "order", symbol, value))
                result.append(self._event(clock, clock, "order", symbol, {**value, "event": "CANCEL"}))
        return result

    def evaluation(self):
        return {"seed": self.seed, "episodes": self.truth,
                "warning": "Expected times apply to uninterrupted feeds. Gaps may delay or truncate episodes."}
