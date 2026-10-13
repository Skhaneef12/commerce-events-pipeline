# Spec: Real-Time Commerce Events Pipeline

The spec comes before the code (spec-driven development). Changes to behaviour start
as a change here, then go to implementation and tests.

## 1. Problem

The product team needs per-customer engagement and conversion metrics
(sessions, events, bookings, revenue, conversion rate). The metrics are built from
clickstream and booking events and refreshed daily. They must be trustworthy:
no duplicates, no silent gaps, and a stated freshness target.

## 2. Data contracts (Kafka topics)

Both topics carry JSON with camelCase keys and are keyed by `customerId`.
Timestamps are ISO-8601 UTC.

### `clickstream.events`
| field | type | rules |
|---|---|---|
| eventId | string | required, unique per event |
| customerId | string | required |
| eventType | string | `page_view` \| `search` \| `add_to_cart` |
| page | string | URL path |
| eventTime | timestamp | required; may arrive up to 10 min late |

### `booking.events`
| field | type | rules |
|---|---|---|
| bookingId | string | required; the business key (the producer may re-send) |
| customerId | string | required |
| amount | double | > 0 |
| currency | string | ISO-4217 |
| status | string | `CONFIRMED` \| `CANCELLED` |
| eventTime | timestamp | required |

Records that fail the required-field rules are dropped at parse time.

## 3. Layers

| layer | table | grain | written by |
|---|---|---|---|
| silver | `customer_sessions` | customer x session (30-min inactivity gap) | StreamingIngestJob |
| silver | `bookings` | booking (deduplicated on `bookingId`) | StreamingIngestJob |
| gold | `daily_customer_metrics` (Delta, partitioned by `metricDate`; mirrored to MongoDB) | customer x day | DailyMetricsJob |

## 4. Functional requirements

- **FR1** Sessionize clicks per customer with a configurable inactivity gap (default 30 min).
- **FR2** Accept events up to the watermark (default 10 min) late. Drop later events, which bounds streaming state.
- **FR3** Count each `bookingId` at most once, even if the producer re-sends it.
- **FR4** Gold counts only `CONFIRMED` bookings. Customers with bookings but no session still appear.
- **FR5** `conversionRate = bookings / sessions`, and 0 when there are no sessions.
- **FR6** Re-running a day gives identical results (idempotent).

## 5. Non-functional requirements (SLIs / SLOs)

| SLI | SLO | enforced by |
|---|---|---|
| Freshness: minutes since the latest session ended | <= 60 min | `FreshnessCheckJob` (Airflow `freshness_slo`) |
| Gold partition built for day D | by 03:00 UTC on D+1 | Airflow schedule 02:00 + 45-min timeout |
| Quality: Error-level Deequ checks pass | 100% of partitions | `QualityJob` (Airflow `quality_gate`) |
| Bookings without a session | <= 1% of customers per day | Deequ `satisfies` threshold |

## 6. Acceptance criteria (each maps to a test)

- [x] Malformed or incomplete JSON is dropped (`TransformationsSpec`: parseClicks)
- [x] A 40-minute gap splits a session; session end = last event + gap (`TransformationsSpec`: sessionize)
- [x] Duplicates removed, cancellations excluded, booking-only customers kept (`TransformationsSpec`: dailyMetrics)
- [x] Same input gives the same output (`TransformationsSpec`: deterministic)
- [x] Duplicate primary key, negative revenue, or a threshold breach fails the gate (`QualityChecksSpec`)
- [x] DAG imports cleanly, retries and `max_active_runs` set as specified (`test_dag_integrity.py`)

## 7. Out of scope (next iterations)

- A Flink or Kafka Streams version of the sessionizer, to compare with Spark
- Schema Registry (Avro/Protobuf) instead of JSON
- Customer identity stitching (anonymous to logged-in)
