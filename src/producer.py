"""Synthetic card-transaction producer.

Generates realistic-looking card swipes and publishes them to Kafka. A small
share of cards are "compromised" and emit bursts, spikes and far-away swipes
so the streaming job has real fraud patterns to catch.
"""
from __future__ import annotations

import argparse
import json
import random
import time
import uuid
from datetime import datetime, timezone

CITIES = {
    "Dallas": (32.7767, -96.7970),
    "Charlotte": (35.2271, -80.8431),
    "Atlanta": (33.7490, -84.3880),
    "New York": (40.7128, -74.0060),
    "Miami": (25.7617, -80.1918),
    "London": (51.5074, -0.1278),
    "Lagos": (6.5244, 3.3792),
}
MCCS = ["5411", "5812", "5541", "5999", "4111", "5732", "4829", "6051", "7995"]


def make_cards(n: int) -> list[dict]:
    home_cities = ["Dallas", "Charlotte", "Atlanta", "New York", "Miami"]
    return [
        {
            "card_id": f"card_{i:05d}",
            "home": random.choice(home_cities),
            "avg": round(random.uniform(20, 180), 2),
            "compromised": random.random() < 0.02,
        }
        for i in range(n)
    ]


def make_event(card: dict) -> dict:
    city = card["home"]
    amount = max(1.0, random.gauss(card["avg"], card["avg"] * 0.4))
    mcc = random.choice(MCCS[:6])
    if card["compromised"] and random.random() < 0.5:
        city = random.choice(["London", "Lagos", "Miami"])
        amount = card["avg"] * random.uniform(6, 20)
        mcc = random.choice(MCCS[6:])
    lat, lon = CITIES[city]
    return {
        "transaction_id": str(uuid.uuid4()),
        "card_id": card["card_id"],
        "amount": round(amount, 2),
        "currency": "USD",
        "merchant_id": f"m_{random.randint(1, 4000):04d}",
        "mcc": mcc,
        "city": city,
        "lat": lat + random.uniform(-0.05, 0.05),
        "lon": lon + random.uniform(-0.05, 0.05),
        "channel": random.choice(["POS", "ECOM", "ATM"]),
        "event_time": datetime.now(timezone.utc).isoformat(),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap", default="localhost:9094")
    parser.add_argument("--topic", default="card-transactions")
    parser.add_argument("--cards", type=int, default=5000)
    parser.add_argument("--rate", type=int, default=200, help="events per second")
    parser.add_argument("--dry-run", action="store_true", help="print instead of sending")
    args = parser.parse_args()

    cards = make_cards(args.cards)
    producer = None
    if not args.dry_run:
        from confluent_kafka import Producer

        producer = Producer({"bootstrap.servers": args.bootstrap, "linger.ms": 20, "acks": "all"})

    interval = 1.0 / args.rate
    sent = 0
    while True:
        card = random.choice(cards)
        burst = 6 if card["compromised"] and random.random() < 0.1 else 1
        for _ in range(burst):
            event = make_event(card)
            payload = json.dumps(event).encode()
            if producer:
                producer.produce(args.topic, key=card["card_id"].encode(), value=payload)
                producer.poll(0)
            else:
                print(payload.decode())
            sent += 1
        if sent % 5000 == 0 and producer:
            producer.flush()
            print(f"sent {sent} events")
        time.sleep(interval)


if __name__ == "__main__":
    main()
