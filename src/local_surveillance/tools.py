"""Read-only investigation functions bound to an immutable availability snapshot.

No access to raw recordings, future data, answer keys or model APIs.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class Investigation:
    store: object
    run_id: str
    as_of_seq: int
    as_of_time: float

    def list_alerts(self, rule=None, limit=100):
        if not 1 <= limit <= 1000:
            raise ValueError("limit must be between 1 and 1000")
        alerts = self.store.alerts(self.run_id, as_of_seq=self.as_of_seq, as_of_time=self.as_of_time)
        return [a for a in alerts if rule is None or a["rule"] == rule][:limit]

    def get_alert(self, alert_id):
        for alert in self.store.alerts(self.run_id, as_of_seq=self.as_of_seq, as_of_time=self.as_of_time):
            if alert["alert_id"] == alert_id:
                return alert
        raise ValueError("Alert unavailable at this cutoff")

    def get_account(self, account_id):
        return self._reference("accounts", account_id)

    def get_evidence(self, event_ids):
        """Resolve alert evidence IDs with the same fixed visibility boundary."""
        if not isinstance(event_ids, list) or not 1 <= len(event_ids) <= 1000:
            raise ValueError("event_ids must be a list of 1 to 1000 IDs")
        wanted = set(event_ids)
        placeholders = ",".join("?" for _ in wanted)
        import json
        rows = self.store.db.execute(
            f"SELECT * FROM events WHERE run_id=? AND seq<=? AND ts<=? AND received_at<=? "
            f"AND event_id IN ({placeholders}) ORDER BY ts,seq",
            [self.run_id, self.as_of_seq, self.as_of_time, self.as_of_time, *sorted(wanted)])
        events = []
        for row in rows:
            value = dict(row)
            value["payload"] = json.loads(value["payload"])
            events.append(value)
        return {"events": events,
                "unavailable_event_ids": sorted(wanted - {e["event_id"] for e in events})}

    def get_trader_profile(self, trader_id):
        return self._reference("traders", trader_id)

    def _reference(self, kind, item_id):
        refs = self.store.references(self.run_id)
        if item_id not in refs.get(kind, {}):
            raise ValueError(f"Unknown {kind}: {item_id}")
        return refs[kind][item_id]

    def get_order_activity(self, start, end, trader_id=None, account=None, symbol=None, limit=200):
        rows = self._events(["order"], start, end, symbol)
        rows = [r for r in rows if (trader_id is None or r["payload"].get("trader_id") == trader_id)
                and (account is None or r["payload"].get("account") == account)]
        return self._limited(rows, limit)

    def get_market_context(self, symbol, start, end, points=10):
        if not 2 <= points <= 100:
            raise ValueError("points must be between 2 and 100")
        quotes = self._events(["market_bbo"], start, end, symbol)
        if not quotes:
            return {"symbol": symbol, "available": False, "reason": "No quotes in visible window"}
        mids = [(float(r["payload"]["bid"]) + float(r["payload"]["ask"])) / 2 for r in quotes]
        indices = sorted({round(i * (len(quotes) - 1) / (points - 1)) for i in range(points)})
        gaps = self._events(["gap"], start, end)
        return {"symbol": symbol, "available": True, "start_mid": mids[0], "end_mid": mids[-1],
                "high_mid": max(mids), "low_mid": min(mids),
                "move_bps": (mids[-1] / mids[0] - 1) * 10000, "gaps": gaps,
                "path": [{"ts": quotes[i]["ts"], "mid": mids[i],
                          "event_id": quotes[i]["event_id"]} for i in indices]}

    def get_supporting_records(self, start, end, trader_id=None, symbol=None, limit=200):
        rows = self._events(["client_order", "client_instruction", "options_position", "algo", "approval", "chat"],
                            start, end, symbol)
        rows = [r for r in rows if trader_id is None or trader_id in
                (r["payload"].get("trader_id"), r["payload"].get("participant"),
                 r["payload"].get("handling_trader"))]
        return self._limited(rows, limit)

    def _events(self, kinds, start, end, symbol=None):
        if end < start:
            raise ValueError("end must not precede start")
        return list(self.store.events(self.run_id, as_of_seq=self.as_of_seq, as_of_time=self.as_of_time,
                                     kinds=kinds, start=start, end=min(end, self.as_of_time), symbol=symbol))

    @staticmethod
    def _limited(rows, limit):
        if not 1 <= limit <= 1000:
            raise ValueError("limit must be between 1 and 1000")
        return {"events_total": len(rows), "events_returned": min(len(rows), limit),
                "truncated": len(rows) > limit, "events": rows[:limit]}
