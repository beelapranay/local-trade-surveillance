"""Incremental, evidence-only versions of the four original surveillance rules.

Alerts indicate suspicion, not a legal conclusion or proof of market impact.
No source CSVs, global clock, model or network access is used here.
"""

from collections import defaultdict, deque
from dataclasses import dataclass, field

from .models import Event, stable_id


@dataclass(frozen=True)
class Thresholds:
    layering_lookback: float = 60
    layering_cancel_window: float = 30
    layering_min_orders: int = 4
    layering_qty_ratio: float = 3
    layering_cancel_fraction: float = .8
    layering_merge_gap: float = 120
    cancel_bucket: float = 900
    cancel_min_new: int = 40
    cancel_ratio: float = .96
    cancel_max_fills: int = 1
    wash_merge_gap: float = 900
    front_running_window: float = 1800


@dataclass
class Order:
    order_id: str
    trader_id: str
    account: str
    symbol: str
    side: str
    ts: float
    qty: float
    price: float
    new_event: str
    filled: float = 0
    cancelled_at: float | None = None
    cancel_event: str | None = None
    fill_events: list = field(default_factory=list)


class Detector:
    def __init__(self, references, thresholds=None):
        self.references = references
        self.p = thresholds or Thresholds()
        self.orders = {}
        self.pending_fills = deque()
        self.clients = {}
        self.cancel_buckets = {}
        self.quotes = defaultdict(deque)
        self.alerts = {}
        self.merge_index = {}
        self.changed = set()
        self.seen = {}
        self.seen_queue = deque()
        self.future = []
        self.errors = []
        self.clock = float("-inf")

    def process(self, event: Event, clock=None):
        now = event.received_at if clock is None else clock
        if now < self.clock:
            raise ValueError("Availability clock cannot move backwards")
        self.clock = now
        if event.kind not in {"market_bbo", "client_order", "order"}:
            return
        if event.event_id in self.seen:
            return
        self.seen[event.event_id] = now
        self.seen_queue.append((now, event.event_id))
        if event.ts > now or event.received_at > now:
            self.future.append(event)
            return
        self._apply(event)

    def _apply(self, event):
        d = event.payload
        if event.kind == "market_bbo":
            quotes = self.quotes[event.symbol]
            quote = (event.ts, (float(d["bid"]) + float(d["ask"])) / 2, event.event_id)
            # Rules need historical midpoints, not every depth update. Store retains all updates.
            if quotes and int(quotes[-1][0]) == int(event.ts):
                quotes[-1] = quote
            else:
                quotes.append(quote)
        elif event.kind == "client_order":
            if d["event"] == "RECEIVED":
                self.clients[d["client_order_id"]] = {**d, "ts": event.ts,
                    "symbol": event.symbol, "event_id": event.event_id, "completed_at": None}
            elif d["event"] == "COMPLETED" and d["client_order_id"] in self.clients:
                self.clients[d["client_order_id"]]["completed_at"] = event.ts
        elif event.kind == "order":
            self._order(event)

    def _order(self, event):
        d, p = event.payload, self.p
        oid = d["order_id"]
        key = (d["trader_id"], d["account"], event.symbol,
               int(event.ts // p.cancel_bucket))
        if d["event"] == "NEW":
            if oid in self.orders or float(d["qty"]) <= 0:
                self.errors.append({"event_id": event.event_id, "reason": "duplicate order or invalid size"})
                return
            self.orders[oid] = Order(oid, d["trader_id"], d["account"], event.symbol, d["side"],
                                    event.ts, float(d["qty"]), float(d["price"]), event.event_id)
            bucket = self.cancel_buckets.setdefault(key, {"new": set(), "cancel": set(),
                                                          "fills": set(), "evidence": set()})
            bucket["new"].add(oid); bucket["evidence"].add(event.event_id)
            return
        order = self.orders.get(oid)
        if not order or event.ts < order.ts or (order.trader_id, order.account, order.symbol, order.side) != (
                d["trader_id"], d["account"], event.symbol, d["side"]):
            self.errors.append({"event_id": event.event_id, "reason": "missing or inconsistent order"})
            return
        bucket = self.cancel_buckets.get((order.trader_id, order.account, order.symbol,
                                          int(order.ts // p.cancel_bucket)))
        if d["event"] == "CANCEL":
            if order.cancelled_at is not None or order.filled >= order.qty:
                self.errors.append({"event_id": event.event_id, "reason": "order already terminated"})
                return
            order.cancelled_at, order.cancel_event = event.ts, event.event_id
            if bucket:
                bucket["cancel"].add(oid); bucket["evidence"].add(event.event_id)
        elif d["event"] == "FILL":
            qty = float(d["qty"])
            if qty <= 0 or order.cancelled_at is not None or order.filled + qty > order.qty + 1e-9:
                self.errors.append({"event_id": event.event_id, "reason": "invalid fill quantity or terminated order"})
                return
            order.filled += qty
            order.fill_events.append(event.event_id)
            if bucket:
                bucket["fills"].add(oid); bucket["evidence"].add(event.event_id)
            if d.get("contra_account") == "EXT":
                self.pending_fills.append(event)
            self._wash(event)
            self._front_running(event)

    def _emit(self, rule, trader_id, account, symbol, start, end, evidence, order_ids, metrics,
              merge_gap=0, discriminator=None):
        key = (rule, trader_id, account, symbol, discriminator)
        previous = self.merge_index.get(key)
        if previous and start - previous["window_end"] <= merge_gap:
            alert = previous
            alert["window_start"] = min(start, alert["window_start"])
            alert["window_end"] = max(end, alert["window_end"])
            alert["evidence_event_ids"] = sorted(set(alert["evidence_event_ids"]) | set(evidence))
            alert["order_ids"] = sorted(set(alert["order_ids"]) | set(order_ids))
            alert["n_triggers"] += 1
            # Preserve all trigger metrics rather than misleading max() aggregation.
            alert["triggers"].append({"start": start, "end": end, "metrics": metrics})
        else:
            aid = stable_id(rule, trader_id, account, symbol, start, discriminator)
            alert = {"alert_id": aid, "rule": rule, "trader_id": trader_id, "account": account,
                     "symbol": symbol, "window_start": start, "window_end": end,
                     "raised_at": self.clock, "updated_at": self.clock, "status": "open",
                     "n_triggers": 1, "metrics": metrics,
                     "triggers": [{"start": start, "end": end, "metrics": metrics}],
                     "order_ids": sorted(set(order_ids)), "evidence_event_ids": sorted(set(evidence)),
                     "activity_source": "synthetic", "assessment": "suspicious_activity",
                     "market_impact_proven": False}
            self.alerts[aid] = alert
            self.merge_index[key] = alert
        alert["updated_at"] = self.clock
        self.changed.add(alert["alert_id"])

    def _mid(self, symbol, ts):
        candidates = [q for q in self.quotes[symbol] if q[0] <= ts]
        return max(candidates, key=lambda q: q[0]) if candidates else None

    def _layering(self, fill):
        d, p = fill.payload, self.p
        account = self.references.get("accounts", {}).get(d["account"], {})
        if account.get("type") == "client":
            return
        opposite = "SELL" if d["side"] == "BUY" else "BUY"
        cand = [o for o in self.orders.values() if o.trader_id == d["trader_id"]
                and o.account == d["account"] and o.symbol == fill.symbol and o.side == opposite
                and fill.ts - p.layering_lookback <= o.ts <= fill.ts]
        cancelled = [o for o in cand if o.cancelled_at is not None
                     and fill.ts <= o.cancelled_at <= fill.ts + p.layering_cancel_window]
        if len(cand) < p.layering_min_orders:
            return
        ratio = sum(o.qty for o in cand) / float(d["qty"])
        fraction = len(cancelled) / len(cand)
        if ratio < p.layering_qty_ratio or fraction < p.layering_cancel_fraction:
            return
        start = min(o.ts for o in cand)
        q0, q1 = self._mid(fill.symbol, start), self._mid(fill.symbol, fill.ts)
        move = ((q1[1] / q0[1] - 1) * (1 if d["side"] == "SELL" else -1) * 10000
                if q0 and q1 else None)
        evidence = [fill.event_id] + [o.new_event for o in cand] + [o.cancel_event for o in cancelled]
        if q0 and q1:
            evidence.extend((q0[2], q1[2]))
        self._emit("LAYERING", d["trader_id"], d["account"], fill.symbol, start,
                   max(fill.ts, max(o.cancelled_at for o in cancelled)), evidence,
                   [d["order_id"]] + [o.order_id for o in cand],
                   {"opposite_orders": len(cand), "qty_ratio": ratio, "cancel_fraction": fraction,
                    "exec_side": d["side"], "exec_qty": float(d["qty"]), "favourable_move_bps": move},
                   p.layering_merge_gap)

    def _wash(self, fill):
        d = fill.payload
        if d["side"] != "SELL" or d.get("contra_account") in {None, "EXT"}:
            return
        accounts = self.references.get("accounts", {})
        a, b = accounts.get(d["account"], {}), accounts.get(d["contra_account"], {})
        owner = a.get("controlled_by")
        if not owner or owner != b.get("controlled_by"):
            return
        pair = "|".join(sorted((d["account"], d["contra_account"])))
        self._emit("WASH_TRADE", d["trader_id"], pair, fill.symbol, fill.ts, fill.ts,
                   [self.orders[d["order_id"]].new_event, fill.event_id], [d["order_id"]],
                   {"qty": float(d["qty"]), "controlled_by": owner}, self.p.wash_merge_gap)

    def _front_running(self, fill):
        d = fill.payload
        traders = self.references.get("traders", {})
        accounts = self.references.get("accounts", {})
        if accounts.get(d["account"], {}).get("type") == "client":
            return
        desk = traders.get(d["trader_id"], {}).get("desk")
        if not desk:
            return
        for client in self.clients.values():
            if (client["symbol"] != fill.symbol or client["side"] != d["side"]
                    or client["account"] == d["account"]):
                continue
            handler_desk = traders.get(client["handling_trader"], {}).get("desk")
            end = min(client["ts"] + self.p.front_running_window,
                      client["completed_at"] if client["completed_at"] is not None else float("inf"))
            if desk == handler_desk and client["ts"] <= fill.ts <= end:
                self._emit("FRONT_RUNNING", d["trader_id"], d["account"], fill.symbol,
                           client["ts"], fill.ts,
                           [client["event_id"], self.orders[d["order_id"]].new_event, fill.event_id],
                           [d["order_id"]], {"client_order_id": client["client_order_id"],
                               "client_qty": client["qty"], "own_qty": float(d["qty"]),
                               "seconds_after_receipt": fill.ts - client["ts"]},
                           self.p.front_running_window, client["client_order_id"])

    def advance(self, clock):
        if clock < self.clock:
            raise ValueError("Availability clock cannot move backwards")
        self.clock = clock
        remaining = []
        for event in self.future:
            if event.ts <= clock and event.received_at <= clock:
                self._apply(event)
            else:
                remaining.append(event)
        self.future = remaining
        pending = deque()
        for fill in self.pending_fills:
            if fill.ts + self.p.layering_cancel_window <= clock:
                self._layering(fill)
            else:
                pending.append(fill)
        self.pending_fills = pending
        for key, bucket in list(self.cancel_buckets.items()):
            trader, account, symbol, number = key
            end = (number + 1) * self.p.cancel_bucket
            if end > clock:
                continue
            n, c, f = len(bucket["new"]), len(bucket["cancel"]), len(bucket["fills"])
            if (self.references.get("accounts", {}).get(account, {}).get("type") != "client"
                    and n >= self.p.cancel_min_new and c / n >= self.p.cancel_ratio
                    and f <= self.p.cancel_max_fills):
                self._emit("EXCESSIVE_CANCELS", trader, account, symbol,
                           number * self.p.cancel_bucket, end, bucket["evidence"], bucket["new"],
                           {"new_orders": n, "cancelled_orders": c, "filled_orders": f,
                            "cancel_ratio": c / n}, discriminator=number)
            del self.cancel_buckets[key]
        retention = max(self.p.front_running_window, self.p.cancel_bucket,
                        self.p.layering_lookback + self.p.layering_cancel_window) + 60
        before = clock - retention
        self.orders = {k: v for k, v in self.orders.items() if v.ts >= before
                       or (v.cancelled_at is None and v.filled < v.qty)}
        self.clients = {k: v for k, v in self.clients.items() if v["ts"] >= before}
        while self.seen_queue and self.seen_queue[0][0] < before:
            _, event_id = self.seen_queue.popleft()
            self.seen.pop(event_id, None)
        for quotes in self.quotes.values():
            while len(quotes) > 1 and quotes[1][0] < before:
                quotes.popleft()
        changed = [self.alerts[k] for k in sorted(self.changed)]
        self.changed.clear()
        return changed


def detect(events, references, cutoff, thresholds=None):
    """Batch facade with exactly the same availability semantics as streaming."""
    detector = Detector(references, thresholds)
    for event in sorted(events, key=lambda e: e.received_at):
        if event.received_at <= cutoff:
            detector.process(event)
    detector.advance(cutoff)
    return list(detector.alerts.values())
