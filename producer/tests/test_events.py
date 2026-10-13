import random
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from events import (BOOKING_STATUSES, customer_ids, event_time, iso,  # noqa: E402
                    make_booking, make_click, to_record)

NOW = datetime(2026, 10, 12, 10, 0, 0, 123456, tzinfo=timezone.utc)


def test_iso_format_is_utc_millis():
    assert iso(NOW) == "2026-10-12T10:00:00.123Z"


def test_click_has_expected_fields_and_customer():
    rec = to_record(make_click("C00001", NOW, random.Random(1)))
    assert set(rec) == {"eventId", "customerId", "eventType", "page", "eventTime"}
    assert rec["customerId"] == "C00001"


def test_generation_is_deterministic_with_seed():
    a = make_click("C00001", NOW, random.Random(42))
    b = make_click("C00001", NOW, random.Random(42))
    assert a == b


def test_late_events_are_never_in_the_future_and_within_bound():
    rng = random.Random(7)
    for _ in range(1000):
        ts = event_time(NOW, rng, late_ratio=0.5, max_late_minutes=8)
        assert NOW - timedelta(minutes=8) <= ts <= NOW


def test_no_late_events_when_ratio_zero():
    rng = random.Random(7)
    assert all(event_time(NOW, rng, 0.0, 8) == NOW for _ in range(100))


def test_booking_amount_positive_and_status_valid():
    rng = random.Random(3)
    for _ in range(200):
        b = make_booking("C00002", NOW, rng)
        assert b.amount > 0
        assert b.status in BOOKING_STATUSES


def test_customer_ids_are_zero_padded_and_unique():
    ids = customer_ids(150)
    assert ids[0] == "C00001" and ids[-1] == "C00150"
    assert len(set(ids)) == 150
