# Local trade surveillance

Python ingestion, replay, simulated internal activity, surveillance rules and
read-only investigation tools. Everything runs on this machine except retrieving
public market data. No orders are submitted, and no LLM or Telegram calls occur.

Real Kraken spot trades and order books provide market context. Fictional traders,
accounts, ownership, client instructions and order lifecycles provide controlled
surveillance scenarios. Public trades keep `trader_id=null`,
`wallet_address=null`, and `identity_source=unknown`. Synthetic events have
`source=synthetic`; they are separate records, not attributed public executions.

## Setup

```bash
cd /home/dell/local-trade-surveillance
python3 -m venv .venv
.venv/bin/pip install -e .
.venv/bin/python -m unittest discover -s tests -v
```

The preinstalled `.venv` is ready to use. To reproduce its exact runtime dependency,
install `requirements-lock.txt`, then install this project with `--no-deps`.

Python 3.12+ is required. `websockets` is the only runtime dependency; detection,
SQLite storage, simulation and investigation otherwise use the standard library.

## Record and replay

```bash
# Capture a real public feed. No API key or exchange account is needed.
.venv/bin/surveillance record --seconds 1200 --symbols BTC/USD ETH/USD

# Find the recording run ID printed by record (or listed here).
.venv/bin/surveillance runs

# Replay that recording with simulated internal activity, at 10x by default.
.venv/bin/surveillance replay --source-run RECORDING_RUN_ID

# Unpaced replay, or resume an interrupted replay with the same seed.
.venv/bin/surveillance replay --source-run RECORDING_RUN_ID --speed 0
.venv/bin/surveillance replay --source-run RECORDING_RUN_ID --resume-run REPLAY_RUN_ID

# Alternatively combine the real feed, simulator and detector in one live run.
.venv/bin/surveillance live --seconds 1200
```

`--db PATH` is a global option and precedes the subcommand. Default storage is
`data/surveillance.sqlite`. Each recording/replay/live run is isolated by run ID.
Replay saves its cursor and derived events/alerts atomically. Restart rebuilds
state from the source prefix and commits only the unprocessed suffix. Ctrl-C
preserves committed progress. Run configuration must match when resuming.

Recording/replay does not extend market data beyond EOF. A short recording may
end with pending cancellation windows or incomplete scenarios. Capture 20 minutes
to exercise the 15-minute cancellation bucket; gaps can delay scenarios further.
Simulated time follows received timestamps; wall-clock replay speed does not alter
event chronology. `--speed 0` removes waiting. Ctrl-C stops a paced replay.

Books use Decimal values, top-10 CRC32 checks and subscribed-depth truncation.
Checksum errors or lost connections create gap events and require fresh snapshots.
Simulation stops until both symbols have recent validated quotes (30-second
freshness limit). Subscription rejection and exhausted reconnection attempts are
visible errors, not silent empty results.

## Rules and scenarios

| Rule | Initial thresholds / evidence |
|---|---|
| Layering | At least four opposite orders in 60s, quantity ratio ≥3, at least 80% cancelled in the 30s **after** execution |
| Excessive cancellations | Completed UTC-aligned 15-minute placement cohort, at least 40 orders, cancellation ratio ≥96%, at most one filled order |
| Wash trading | Sell-side cross between accounts with known matching controllers; merge related crosses over 15 minutes |
| Front-running | Same-side own fill on the client's symbol by the handling desk, after client receipt and before completion or 30 minutes |

Rules are deliberately loose. Partial fills count once per order for the
cancellation rule; duplicate event IDs do not double-count. Unknown ownership
never matches another unknown owner. Layering waits for its post-fill evidence
window. Alerts merge related triggers and retain event IDs and trigger metrics.

Included scenarios: suspicious layering, linked-account crosses and personal
trading ahead of a client; legitimate delta hedging, instructed fund crosses,
pre-approved TWAP execution and market-maker cancellations. They are examples
for investigation, not proof of a real exchange participant's misconduct.

Simulated orders do not change the external market. A measured price move does
not establish that a fictional order caused it. The feed has price-level depth,
not market-wide identifiable order lifecycles. Symbols remain `BTC/USD` and
`ETH/USD`; they are not relabelled as perpetual futures.

## Python functions and investigation tools

```python
from local_surveillance import Detector, detect
from local_surveillance.store import Store
from local_surveillance.tools import Investigation

# Batch: normalized Event objects and explicit reference data.
alerts = detect(events, references, cutoff=available_time)

# Streaming: availability must advance monotonically.
detector = Detector(references)
detector.process(event)
changed_alerts = detector.advance(available_time)  # pipeline calls every five seconds

# Bind an investigation to an immutable cutoff, before retrieving evidence.
store = Store("data/surveillance.sqlite")
tools = Investigation(store, run_id, as_of_seq=sequence, as_of_time=available_time)
alerts = tools.list_alerts()
case = tools.get_alert(alert_id)
evidence = tools.get_evidence(case["evidence_event_ids"])
orders = tools.get_order_activity(start, end, trader_id="T03")
market = tools.get_market_context("ETH/USD", start, end)
account = tools.get_account("PERSONAL")
profile = tools.get_trader_profile("T03")
support = tools.get_supporting_records(start, end, trader_id="T03")
store.close()
```

All event reads enforce ingestion sequence, event timestamp and receipt timestamp.
Alert versions also respect the snapshot. Supporting records only appear after
receipt; no full-day averages or future client fills are exposed. Static identity
references are installed at run creation and represent fictional known records.
Tool results include source/provenance and report truncation. Chat text is
untrusted evidence, never an instruction to a future agent.

```bash
.venv/bin/surveillance inspect --run REPLAY_RUN_ID
.venv/bin/surveillance inspect --run REPLAY_RUN_ID --tool get_alert \
  --args '{"alert_id":"ALERT_ID"}'
.venv/bin/surveillance detect --run REPLAY_RUN_ID
```

`detect` recomputes from stored events and rule-evaluation clock markers. An alert
means **suspicious activity**, not a fraud verdict. Agent decisions and human
review are intentionally a later integration.

## Evaluation and throughput

Replay writes scenario answer keys under `eval_data/RUN_ID.json`, outside the
database. Investigation functions do not expose this directory or scenario
labels. Scenario timings assume uninterrupted feeds; gaps can invalidate timing
matches. Short captures leave cases pending rather than fabricate future evidence.

```bash
.venv/bin/surveillance benchmark --source-run RECORDING_RUN_ID \
  --rates 100 1000 10000 --seconds 10 --output benchmark-results.json
```

The benchmark repeats a bounded real recording, explicitly remaps timestamps to
a 100x simulation clock, and assigns replay-instance event IDs so repetition
is not dropped as duplicate input. It measures JSON normalization, CRC validation,
raw/normalized SQLite writes, internal simulation and rule evaluation in batches
of 200 market events. Results include achieved delivery rate, processing capacity,
pause/error counts and whether each target kept up. This is a finite local stress
test, not a claim about network capacity, actual exchange activity, unlimited
retention, detection accuracy or model investigations per minute. Recording uses
per-message transactions; batched stress throughput is reported separately.

Tests cover the official checksum example, resynchronization, attribution,
each rule, partial fills, unknown ownership, future-data masking, deterministic
resume, duplicate persistence and availability-scoped alert versions. Full
scenario tests use clearly synthetic book fixtures; live-feed checks and
benchmarks use captured Kraken data.

No cloud inference, dashboard, model deployment, Telegram configuration or
modification of the original `/home/dell/trade-surveillance` checkout is included.
See [ATTRIBUTION.md](ATTRIBUTION.md) for the original project and protocol sources.
