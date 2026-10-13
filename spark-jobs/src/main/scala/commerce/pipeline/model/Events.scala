package commerce.pipeline.model

import java.sql.{Date, Timestamp}

/** Raw click event from the `clickstream.events` topic (data contract: docs/spec.md). */
final case class ClickEvent(
    eventId: String,
    customerId: String,
    eventType: String,
    page: String,
    eventTime: Timestamp
)

/** Raw booking event from the `booking.events` topic. */
final case class BookingEvent(
    bookingId: String,
    customerId: String,
    amount: Double,
    currency: String,
    status: String,
    eventTime: Timestamp
)

/** Silver: one row per customer session (30-minute inactivity gap by default). */
final case class CustomerSession(
    customerId: String,
    sessionStart: Timestamp,
    sessionEnd: Timestamp,
    eventCount: Long,
    pageViews: Long,
    addToCarts: Long
)

/** Gold: one row per customer per day. Primary key: (metricDate, customerId). */
final case class DailyCustomerMetric(
    metricDate: Date,
    customerId: String,
    sessions: Long,
    events: Long,
    bookings: Long,
    revenue: Double,
    conversionRate: Double
)

object BookingStatus {
  val Confirmed = "CONFIRMED"
  val Cancelled = "CANCELLED"
}
