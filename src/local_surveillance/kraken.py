"""Kraken public WebSocket v2 normalization, with decimal-safe book checksums."""

import json
from decimal import Decimal
import zlib

from .models import Event, stable_id, timestamp


class BookError(ValueError):
    pass


def decimal(value):
    return value if isinstance(value, Decimal) else Decimal(str(value))


def checksum_component(value):
    return format(decimal(value), "f").replace(".", "").lstrip("0")


class Book:
    def __init__(self, depth=10):
        self.depth = depth
        self.bids = {}
        self.asks = {}
        self.ready = False

    def checksum(self):
        parts = []
        for levels, reverse in ((self.asks, False), (self.bids, True)):
            for price in sorted(levels, reverse=reverse)[:10]:
                stored_price, qty = levels[price]
                parts.extend((checksum_component(stored_price), checksum_component(qty)))
        return zlib.crc32("".join(parts).encode()) & 0xffffffff

    def apply(self, data, snapshot=False):
        if snapshot:
            self.bids.clear(); self.asks.clear(); self.ready = False
        elif not self.ready:
            raise BookError("Update received before a valid snapshot")
        for side in ("bids", "asks"):
            levels = getattr(self, side)
            for item in data.get(side, []):
                price, qty = decimal(item["price"]), decimal(item["qty"])
                if qty < 0 or price <= 0:
                    raise BookError("Invalid price level")
                if qty == 0:
                    levels.pop(price, None)
                else:
                    # Keep the most recent decimal representation, including trailing zeros.
                    levels[price] = (price, qty)
            keep = set(sorted(levels, reverse=side == "bids")[:self.depth])
            for price in list(levels):
                if price not in keep:
                    del levels[price]
        if "checksum" not in data or self.checksum() != int(data["checksum"]):
            self.ready = False
            raise BookError("Order-book checksum mismatch")
        if not self.bids or not self.asks or max(self.bids) >= min(self.asks):
            self.ready = False
            raise BookError("Empty or crossed order book")
        self.ready = True

    def bbo(self):
        if not self.ready:
            raise BookError("Book unavailable")
        bid, ask = max(self.bids), min(self.asks)
        return {"bid": str(self.bids[bid][0]), "ask": str(self.asks[ask][0]),
                "bid_size": str(self.bids[bid][1]), "ask_size": str(self.asks[ask][1])}


class Normalizer:
    def __init__(self, depth=10):
        self.depth = depth
        self.books = {}

    def reset(self):
        self.books.clear()

    def normalize(self, raw, received_at, connection_id, raw_seq):
        message = json.loads(raw, parse_float=Decimal)
        if message.get("type") == "local_gap":
            self.reset()
            return [Event(stable_id("gap", connection_id, raw_seq), received_at, received_at,
                          "gap", None, "system", {"reason": message["reason"]})]
        if message.get("success") is False:
            raise ValueError(f"Kraken subscription rejected: {message.get('error')}")
        channel = message.get("channel")
        events = []
        for i, data in enumerate(message.get("data", []) if channel in {"book", "trade"} else []):
            symbol = data["symbol"]
            ts = timestamp(data.get("timestamp", received_at))
            if channel == "book":
                book = self.books.setdefault(symbol, Book(self.depth))
                book.apply(data, snapshot=message.get("type") == "snapshot")
                identity = stable_id("kraken", connection_id, raw_seq, i)
                levels = {k: data.get(k, []) for k in ("bids", "asks")}
                levels.update({"checksum": int(data["checksum"]), "snapshot": message.get("type") == "snapshot"})
                events.append(Event(identity, ts, received_at, "book_delta", symbol, "kraken", levels))
                events.append(Event(stable_id(identity, "bbo"), ts, received_at,
                                    "market_bbo", symbol, "kraken", book.bbo()))
            else:
                payload = {k: data[k] for k in ("trade_id", "side", "price", "qty", "ord_type")}
                payload.update({"trader_id": None, "wallet_address": None,
                                "identity_source": "unknown"})
                events.append(Event(stable_id("kraken-trade", symbol, data["trade_id"]),
                                    ts, received_at, "market_trade", symbol, "kraken", payload))
        return events
