"""Pure event-generation logic for the commerce events producer.

Kept free of Kafka imports so it can be unit-tested without a broker.
Events are JSON with camelCase keys matching the Scala case classes in
spark-jobs/src/main/scala/commerce/pipeline/model/Events.scala.
"""
from __future__ import annotations

import random
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone

CLICK_TOPIC = "clickstream.events"
BOOKING_TOPIC = "booking.events"

PAGES = ["/", "/search", "/product", "/product/detail", "/cart", "/checkout"]
EVENT_TYPES = ["page_view", "page_view", "page_view", "search", "add_to_cart"]
CURRENCIES = ["USD", "USD", "USD", "EUR"]
BOOKING_STATUSES = ["CONFIRMED", "CONFIRMED", "CONFIRMED", "CANCELLED"]


def iso(ts: datetime) -> str:
    """ISO-8601 UTC with millisecond precision, e.g. 2026-10-13T04:20:57.068Z."""
    return ts.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.") + f"{ts.microsecond // 1000:03d}Z"


@dataclass(frozen=True)
class ClickEvent:
    eventId: str
    customerId: str
    eventType: str
    page: str
    eventTime: str


@dataclass(frozen=True)
class BookingEvent:
    bookingId: str
    customerId: str
    amount: float
    currency: str
    status: str
    eventTime: str


def event_time(now: datetime, rng: random.Random, late_ratio: float, max_late_minutes: int) -> datetime:
    """Return `now`, or (with probability late_ratio) a time up to max_late_minutes in the past.

    Late events exist on purpose: they exercise the Spark watermark logic.
    """
    if rng.random() < late_ratio:
        return now - timedelta(minutes=rng.uniform(1, max_late_minutes))
    return now


def make_click(customer_id: str, now: datetime, rng: random.Random,
               late_ratio: float = 0.05, max_late_minutes: int = 8) -> ClickEvent:
    return ClickEvent(
        eventId=str(uuid.UUID(int=rng.getrandbits(128))),
        customerId=customer_id,
        eventType=rng.choice(EVENT_TYPES),
        page=rng.choice(PAGES),
        eventTime=iso(event_time(now, rng, late_ratio, max_late_minutes)),
    )


def make_booking(customer_id: str, now: datetime, rng: random.Random) -> BookingEvent:
    return BookingEvent(
        bookingId=str(uuid.UUID(int=rng.getrandbits(128))),
        customerId=customer_id,
        amount=round(rng.uniform(20, 900), 2),
        currency=rng.choice(CURRENCIES),
        status=rng.choice(BOOKING_STATUSES),
        eventTime=iso(now),
    )


def customer_ids(n: int) -> list[str]:
    return [f"C{i:05d}" for i in range(1, n + 1)]


def to_record(event: ClickEvent | BookingEvent) -> dict:
    return asdict(event)
