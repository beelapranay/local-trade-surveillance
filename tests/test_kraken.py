from decimal import Decimal
import json
import unittest

from local_surveillance.kraken import Book, BookError, Normalizer
from local_surveillance.models import Event
from helpers import book_message


class KrakenTests(unittest.TestCase):
    def test_official_checksum_example(self):
        bids = [("45283.5", "0.10000000"), ("45283.4", "1.54582015"),
                ("45282.1", "0.10000000"), ("45281.0", "0.10000000"),
                ("45280.3", "1.54592586"), ("45279.0", "0.07990000"),
                ("45277.6", "0.03310103"), ("45277.5", "0.30000000"),
                ("45277.3", "1.54602737"), ("45276.6", "0.15445238")]
        asks = [("45285.2", "0.00100000"), ("45286.4", "1.54571953"),
                ("45286.6", "1.54571109"), ("45289.6", "1.54560911"),
                ("45290.2", "0.15890660"), ("45291.8", "1.54553491"),
                ("45294.7", "0.04454749"), ("45296.1", "0.35380000"),
                ("45297.5", "0.09945542"), ("45299.5", "0.18772827")]
        data = {side: [{"price": p, "qty": q} for p, q in pairs]
                for side, pairs in (("bids", bids), ("asks", asks))}
        data["checksum"] = 3310070434
        book = Book()
        book.apply(data, snapshot=True)
        self.assertEqual(book.checksum(), 3310070434)
        self.assertEqual(book.bbo()["bid"], "45283.5")

    def test_fail_closed_and_resnapshot(self):
        n = Normalizer()
        n.normalize(book_message(), 1, "c1", 1)
        bad = json.loads(book_message(snapshot=False)); bad["data"][0]["checksum"] = 0
        with self.assertRaises(BookError):
            n.normalize(json.dumps(bad), 2, "c1", 2)
        with self.assertRaises(BookError):
            n.normalize(book_message(snapshot=False), 3, "c1", 3)
        n.normalize(book_message(), 4, "c2", 4)
        self.assertTrue(n.books["ETH/USD"].ready)

    def test_depth_truncation_and_zero_removal(self):
        b = Book(10)
        b.apply(json.loads(book_message())["data"][0], snapshot=True)
        b.bids[Decimal("99")] = (Decimal("99"), Decimal("1"))
        b.bids.pop(Decimal("100"))
        crc = b.checksum()
        b.bids[Decimal("100")] = (Decimal("100"), Decimal("1.00000000"))
        b.apply({"bids": [{"price": "100", "qty": "0"}], "checksum": crc})
        self.assertEqual(b.bbo()["bid"], "99")

    def test_public_trade_identity_unknown(self):
        raw = json.dumps({"channel": "trade", "type": "update", "data": [
            {"symbol": "ETH/USD", "trade_id": 42, "side": "buy", "price": 123.4,
             "qty": .01, "ord_type": "market", "timestamp": "2026-10-03T13:00:00Z"}]})
        e = Normalizer().normalize(raw, 1800000000, "c", 1)[0]
        self.assertEqual(e.payload["identity_source"], "unknown")
        self.assertIsNone(e.payload["trader_id"])
        with self.assertRaises(ValueError):
            Event("x", 1, 1, "market_trade", "ETH/USD", "kraken", {"trader_id": "T01"})

    def test_subscription_error_is_visible(self):
        with self.assertRaisesRegex(ValueError, "subscription rejected"):
            Normalizer().normalize('{"success":false,"error":"invalid symbol"}', 1, "c", 1)
