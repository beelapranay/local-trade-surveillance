"""Small, explicit interfaces shared by ingestion, detectors and tools."""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json


def timestamp(value):
    if isinstance(value, (int, float, Decimal)):
        return float(value)
    return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()


def utc(value):
    return datetime.fromtimestamp(value, timezone.utc).isoformat().replace("+00:00", "Z")


def dumps(value):
    return json.dumps(value, default=lambda x: str(x) if isinstance(x, Decimal) else _unsupported(x),
                      sort_keys=True, separators=(",", ":"), allow_nan=False)


def _unsupported(value):
    raise TypeError(f"Cannot serialize {type(value).__name__}")


def stable_id(*parts):
    return hashlib.sha256(dumps(parts).encode()).hexdigest()[:24]


@dataclass(frozen=True)
class Event:
    event_id: str
    ts: float
    received_at: float
    kind: str
    symbol: str | None
    source: str
    payload: dict = field(default_factory=dict)

    def __post_init__(self):
        if self.source not in {"kraken", "synthetic", "system"}:
            raise ValueError("Invalid provenance")
        if self.source == "kraken" and any(self.payload.get(k) for k in
                ("trader_id", "account", "contra_account", "wallet_address")):
            raise ValueError("Public market events cannot be assigned fictional identities")

    def as_dict(self):
        return {"event_id": self.event_id, "ts": self.ts, "received_at": self.received_at,
                "kind": self.kind, "symbol": self.symbol, "source": self.source,
                "payload": self.payload}
