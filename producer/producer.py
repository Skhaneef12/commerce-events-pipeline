"""Publish simulated clickstream and booking events to Kafka.

Usage (from repo root, with docker compose up):
    python producer/producer.py --bootstrap localhost:29092 --rate 20 --duration 600

Messages are keyed by customerId, so all events for a customer land on the
same partition and keep their relative order.
"""
from __future__ import annotations

import argparse
import json
import random
import signal
import time
from datetime import datetime, timezone

from confluent_kafka import Producer

from events import (BOOKING_TOPIC, CLICK_TOPIC, customer_ids, make_booking,
                    make_click, to_record)

_running = True


def _stop(*_):
    global _running
    _running = False


def delivery_report(err, msg):
    if err is not None:
        print(f"delivery failed for key={msg.key()}: {err}")


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", default="localhost:29092")
    p.add_argument("--rate", type=float, default=20, help="click events per second")
    p.add_argument("--customers", type=int, default=200)
    p.add_argument("--booking-prob", type=float, default=0.03, help="chance a click is followed by a booking")
    p.add_argument("--dup-ratio", type=float, default=0.02, help="chance a booking is sent twice")
    p.add_argument("--late-ratio", type=float, default=0.05, help="share of clicks with a late eventTime")
    p.add_argument("--duration", type=int, default=0, help="seconds to run; 0 = until Ctrl+C")
    p.add_argument("--seed", type=int, default=None)
    args = p.parse_args()

    rng = random.Random(args.seed)
    customers = customer_ids(args.customers)
    producer = Producer({
        "bootstrap.servers": args.bootstrap,
        "enable.idempotence": True,   # no broker-side duplicates on retry
        "acks": "all",
        "linger.ms": 20,
    })

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)
    started = time.time()
    sent = 0
    interval = 1.0 / args.rate

    while _running and (args.duration == 0 or time.time() - started < args.duration):
        now = datetime.now(timezone.utc)
        cust = rng.choice(customers)
        click = make_click(cust, now, rng, late_ratio=args.late_ratio)
        producer.produce(CLICK_TOPIC, key=cust, value=json.dumps(to_record(click)), callback=delivery_report)
        sent += 1

        if rng.random() < args.booking_prob:
            booking = make_booking(cust, now, rng)
            payload = json.dumps(to_record(booking))
            producer.produce(BOOKING_TOPIC, key=cust, value=payload, callback=delivery_report)
            # Simulate an upstream retry that re-sends the same business event.
            if rng.random() < args.dup_ratio:
                producer.produce(BOOKING_TOPIC, key=cust, value=payload, callback=delivery_report)

        producer.poll(0)
        if sent % 500 == 0:
            print(f"sent {sent} click events")
        time.sleep(interval)

    producer.flush(10)
    print(f"done, sent {sent} click events")


if __name__ == "__main__":
    main()
