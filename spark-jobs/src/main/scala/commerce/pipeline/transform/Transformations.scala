package commerce.pipeline.transform

import commerce.pipeline.model._
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType

/** Pure transformations shared by the streaming and batch jobs.
  *
  * Nothing here reads or writes storage, so every function is unit-testable
  * with a small in-memory Dataset (see TransformationsSpec).
  */
object Transformations {

  val clickSchema: StructType   = Encoders.product[ClickEvent].schema
  val bookingSchema: StructType = Encoders.product[BookingEvent].schema

  /** Kafka `value` bytes -> typed clicks. Malformed JSON or missing keys are dropped. */
  def parseClicks(kafkaDf: DataFrame)(implicit spark: SparkSession): Dataset[ClickEvent] = {
    import spark.implicits._
    kafkaDf
      .select(from_json(col("value").cast("string"), clickSchema).as("e"))
      .select("e.*")
      .where(col("eventId").isNotNull && col("customerId").isNotNull && col("eventTime").isNotNull)
      .as[ClickEvent]
  }

  def parseBookings(kafkaDf: DataFrame)(implicit spark: SparkSession): Dataset[BookingEvent] = {
    import spark.implicits._
    kafkaDf
      .select(from_json(col("value").cast("string"), bookingSchema).as("e"))
      .select("e.*")
      .where(col("bookingId").isNotNull && col("customerId").isNotNull && col("eventTime").isNotNull)
      .as[BookingEvent]
  }

  /** Group clicks into sessions: a session ends after `gap` with no activity.
    *
    * The watermark bounds how late an event may arrive and still be counted. It
    * also lets Spark drop old state, so memory doesn't grow forever. On a batch
    * Dataset (tests, backfills) withWatermark is a no-op.
    */
  def sessionize(clicks: Dataset[ClickEvent], gap: String, watermark: String)(implicit
      spark: SparkSession
  ): Dataset[CustomerSession] = {
    import spark.implicits._
    clicks
      .withWatermark("eventTime", watermark)
      .groupBy(col("customerId"), session_window(col("eventTime"), gap).as("session"))
      .agg(
        count(lit(1)).as("eventCount"),
        sum(when(col("eventType") === "page_view", 1L).otherwise(0L)).as("pageViews"),
        sum(when(col("eventType") === "add_to_cart", 1L).otherwise(0L)).as("addToCarts")
      )
      .select(
        col("customerId"),
        col("session.start").as("sessionStart"),
        col("session.end").as("sessionEnd"),
        col("eventCount"),
        col("pageViews"),
        col("addToCarts")
      )
      .as[CustomerSession]
  }

  /** One row per booking. Upstream retries can re-send the same booking. */
  def dedupeBookings(bookings: Dataset[BookingEvent]): Dataset[BookingEvent] =
    bookings.dropDuplicates("bookingId")

  /** Gold metrics for one day (UTC).
    *
    * A full outer join keeps customers who booked without a tracked session, and
    * customers who browsed without booking.
    */
  def dailyMetrics(sessions: Dataset[CustomerSession], bookings: Dataset[BookingEvent], date: String)(implicit
      spark: SparkSession
  ): Dataset[DailyCustomerMetric] = {
    import spark.implicits._
    val day = to_date(lit(date))

    val sessionAgg = sessions
      .where(to_date(col("sessionStart")) === day)
      .groupBy("customerId")
      .agg(count(lit(1)).as("sessions"), sum("eventCount").as("events"))

    val bookingAgg = dedupeBookings(bookings)
      .where(to_date(col("eventTime")) === day && col("status") === BookingStatus.Confirmed)
      .groupBy("customerId")
      .agg(count(lit(1)).as("bookings"), sum("amount").as("revenue"))

    sessionAgg
      .join(bookingAgg, Seq("customerId"), "full_outer")
      .na.fill(0L, Seq("sessions", "events", "bookings"))
      .na.fill(0.0, Seq("revenue"))
      .select(
        day.as("metricDate"),
        col("customerId"),
        col("sessions"),
        col("events"),
        col("bookings"),
        round(col("revenue"), 2).as("revenue"),
        when(col("sessions") > 0, round(col("bookings") / col("sessions"), 4))
          .otherwise(lit(0.0))
          .as("conversionRate")
      )
      .as[DailyCustomerMetric]
  }
}
