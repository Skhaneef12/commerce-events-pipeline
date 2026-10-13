# AI-Assisted Code Review Log

This project is built with GitHub Copilot and Claude. Generated code isn't merged as-is:
each suggestion is checked for correctness, edge cases, performance, idempotency and
security before it goes in. This log records what was caught.

**How to use this file:** add an entry whenever you change or reject an AI suggestion.
The examples below show the kind of issue to look for in Spark code. Replace them with
your own entries as you build. These entries make good interview stories.

## Review checklist

- [ ] **Correctness:** does it match `docs/spec.md`? Check the join type and what happens to nulls.
- [ ] **State and memory:** does any streaming stateful operator have a watermark?
- [ ] **Performance:** look for `collect()` on large data, Python/Scala UDFs where a built-in exists, unnecessary shuffles, and missing broadcast hints.
- [ ] **Idempotency:** can the job be re-run for the same date without creating duplicates?
- [ ] **Time zones:** are all dates computed in UTC?
- [ ] **Tests:** is there a test that would fail if this logic broke?
- [ ] **Security:** no credentials in code. Config comes from environment variables.

## Entries

| date | file | AI suggested | problem | what I changed |
|---|---|---|---|---|
| _example_ | StreamingIngestJob | `dropDuplicates("bookingId")` on the stream | No watermark, so dedup state grows forever | `withWatermark` + `dropDuplicatesWithinWatermark` |
| _example_ | DailyMetricsJob | `mode("overwrite")` on the gold table | Wipes every date, not just the one being rebuilt | `replaceWhere` on `metricDate` |
| _example_ | Transformations | inner join of sessions and bookings | Drops customers who booked without a tracked session | `full_outer` + `na.fill` |
