# Real-Time Card Fraud Detection: Kafka + Spark Structured Streaming + Delta Lake

A streaming pipeline that scores card transactions for fraud seconds after they happen. Synthetic card swipes flow through Kafka, Spark Structured Streaming enriches and scores them, and results land in a Delta Lake medallion layout (bronze / silver / gold). A Streamlit dashboard shows live volume and alerts.

> Personal portfolio project. All data is synthetic. No real customer or bank data is used.

## Architecture

```
 ┌──────────────┐    ┌─────────┐    ┌───────────────────────────────┐    ┌──────────────┐
 │ Python       │ →  │  Kafka  │ →  │ Spark Structured Streaming    │ →  │ Delta Lake   │
 │ producer     │    │ (KRaft) │    │  • parse + dedupe (watermark) │    │ bronze       │
 │ (synthetic   │    │ 6 parts │    │  • per-card features          │    │ silver       │
 │  swipes)     │    └─────────┘    │  • rule-based risk score      │    │ gold: alerts │
 └──────────────┘                   └───────────────────────────────┘    │       KPIs   │
                                                                         └──────┬───────┘
                                                                                ↓
                                                                        Streamlit dashboard
```

| Layer | Table | What it holds |
| --- | --- | --- |
| Bronze | `bronze/transactions` | Raw Kafka payload + topic/partition/offset, append-only, for replay and audit |
| Silver | `silver/transactions` | Typed, deduplicated transactions, partitioned by `event_date` |
| Gold | `gold/fraud_alerts` | Transactions scored `REVIEW` or `BLOCK`, with the rules that fired |
| Gold | `gold/kpis_per_minute` | Volume, amount, blocked and review counts per minute |

## Fraud rules

| Rule | Logic | Weight |
| --- | --- | --- |
| Amount spike | Amount ≥ 5× the card's 30-day average (or ≥ $2,000 with no history) | 0.35 |
| Velocity | 5+ swipes on the same card within 60 seconds | 0.25 |
| Impossible travel | Consecutive swipes 100+ km apart that imply > 900 km/h | 0.30 |
| High-risk MCC | Wires, quasi-cash/crypto, gambling, telemarketing | 0.10 |

Score ≥ 0.60 → `BLOCK`, ≥ 0.35 → `REVIEW`, otherwise `APPROVE`. The rules live in plain Python (`src/rules.py`) with unit tests, and are mirrored as Spark column expressions in the streaming job.

## Key engineering points

- **Exactly-once-style sinks**: checkpointed Structured Streaming with `foreachBatch` writes to Delta, which is transactional.
- **Late and duplicate events**: 10-minute watermark with `dropDuplicatesWithinWatermark` on `transaction_id`.
- **Stateful features**: per-card 30-day averages read from silver each micro-batch; velocity and travel computed with window functions.
- **Replayability**: bronze keeps raw payloads plus Kafka offsets, so silver/gold can be rebuilt.
- **Partitioning**: silver and gold partitioned by `event_date` for fast pruning.

## Run it

Requirements: Docker and Docker Compose.

```bash
docker compose up --build
# dashboard: http://localhost:8501
```

The producer sends ~200 events/second by default (`--rate` to change). About 2% of cards are compromised and generate bursts, spikes and far-away swipes.

Run the producer without Kafka to see the events:

```bash
pip install -r requirements.txt
python src/producer.py --dry-run
```

## Tests

```bash
pytest -q
```

## Project structure

```
src/
  producer.py        synthetic transaction generator → Kafka
  streaming_job.py   Spark Structured Streaming: bronze, silver, gold
  rules.py           fraud rules and scoring (pure Python, unit-tested)
  dashboard.py       Streamlit dashboard over gold tables
tests/
  test_rules.py
docker-compose.yml   Kafka (KRaft), Spark, producer, dashboard
```

## Next steps

- Replace rule weights with a model (e.g. gradient boosting) trained on silver and served with MLflow.
- Keep per-card state across micro-batches with `applyInPandasWithState` instead of reading silver each batch.
- Deploy on Databricks with Delta Live Tables and Unity Catalog.

## Tech

Python · Apache Kafka · Apache Spark 3.5 (Structured Streaming) · Delta Lake · Streamlit · Docker
