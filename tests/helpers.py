import json

from local_surveillance.kraken import Book
from local_surveillance.models import Event


def order(oid, action, ts, trader="T01", account="PROP", side="BUY", qty=1, contra="EXT", eid=None):
    return Event(eid or f"{oid}-{action}-{ts}", ts, ts, "order", "ETH/USD", "synthetic",
                 {"order_id": oid, "trader_id": trader, "account": account, "side": side,
                  "qty": str(qty), "price": "100", "event": action, "contra_account": contra})


def book_message(symbol="ETH/USD", snapshot=True, price=100):
    data = {"symbol": symbol, "bids": [{"price": str(price), "qty": "1.00000000"}],
            "asks": [{"price": str(price + 1), "qty": "2.00000000"}]}
    b = Book()
    from decimal import Decimal
    b.bids = {Decimal(str(price)): (Decimal(str(price)), Decimal("1.00000000"))}
    b.asks = {Decimal(str(price + 1)): (Decimal(str(price + 1)), Decimal("2.00000000"))}
    data["checksum"] = b.checksum()
    return json.dumps({"channel": "book", "type": "snapshot" if snapshot else "update", "data": [data]})


def recording(store, seconds=1000):
    config = {"symbols": ["BTC/USD", "ETH/USD"], "depth": 10, "simulate": False, "seed": 13}
    run = store.create_run("record", config)
    with store.transaction():
        for second in range(seconds + 1):
            for symbol in config["symbols"]:
                store.save_raw(run, float(second), "c1", book_message(symbol, snapshot=second == 0))
    store.set_status(run, "complete")
    return run
