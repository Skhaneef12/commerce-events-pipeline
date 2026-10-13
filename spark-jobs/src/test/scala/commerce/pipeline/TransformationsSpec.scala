package commerce.pipeline

import commerce.pipeline.jobs.FreshnessCheckJob
import commerce.pipeline.model._
import commerce.pipeline.transform.Transformations
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.sql.Timestamp
import java.time.Instant

class TransformationsSpec extends AnyFunSuite with Matchers with SparkTestSession {
  import spark.implicits._

  private def ts(s: String): Timestamp = Timestamp.valueOf(s) // JVM runs in UTC (see build.sbt)

  private def click(cust: String, time: String, eventType: String = "page_view") =
    ClickEvent(java.util.UUID.randomUUID().toString, cust, eventType, "/", ts(time))

  private def booking(id: String, cust: String, amount: Double, time: String, status: String = BookingStatus.Confirmed) =
    BookingEvent(id, cust, amount, "USD", status, ts(time))

  test("parseClicks keeps valid JSON and drops malformed or incomplete records") {
    val raw = Seq(
      """{"eventId":"e1","customerId":"C1","eventType":"page_view","page":"/","eventTime":"2026-10-12T10:00:00.000Z"}""",
      """{"eventId":"e2","customerId":"C1"}""", // missing eventTime
      """not json at all"""
    ).toDF("value")

    val parsed = Transformations.parseClicks(raw).collect()
    parsed.map(_.eventId) should contain only "e1"
    parsed.head.eventTime shouldBe ts("2026-10-12 10:00:00")
  }

  test("sessionize starts a new session after a 30 minute gap") {
    val clicks = Seq(
      click("C1", "2026-10-12 10:00:00"),
      click("C1", "2026-10-12 10:10:00", "add_to_cart"),
      click("C1", "2026-10-12 10:50:00"), // 40 min after the previous event -> new session
      click("C2", "2026-10-12 10:05:00")
    ).toDS()

    val sessions = Transformations
      .sessionize(clicks, "30 minutes", "10 minutes")
      .collect()
      .sortBy(s => (s.customerId, s.sessionStart.getTime))

    sessions.map(s => (s.customerId, s.eventCount)) shouldBe Array(("C1", 2L), ("C1", 1L), ("C2", 1L))

    val first = sessions.head
    first.sessionStart shouldBe ts("2026-10-12 10:00:00")
    first.sessionEnd shouldBe ts("2026-10-12 10:40:00") // last event + gap
    first.pageViews shouldBe 1L
    first.addToCarts shouldBe 1L
  }

  test("dailyMetrics dedupes bookings, ignores cancellations and keeps booking-only customers") {
    val sessions = Seq(
      CustomerSession("C1", ts("2026-10-12 10:00:00"), ts("2026-10-12 10:40:00"), 5, 4, 1),
      CustomerSession("C1", ts("2026-10-12 18:00:00"), ts("2026-10-12 18:30:00"), 3, 3, 0),
      CustomerSession("C2", ts("2026-10-12 09:00:00"), ts("2026-10-12 09:30:00"), 2, 2, 0),
      CustomerSession("C1", ts("2026-10-11 10:00:00"), ts("2026-10-11 10:30:00"), 9, 9, 0) // other day
    ).toDS()

    val bookings = Seq(
      booking("b1", "C1", 100.0, "2026-10-12 10:20:00"),
      booking("b1", "C1", 100.0, "2026-10-12 10:20:00"), // duplicate re-send
      booking("b2", "C2", 50.0, "2026-10-12 09:15:00", BookingStatus.Cancelled),
      booking("b3", "C3", 75.5, "2026-10-12 12:00:00") // no tracked session
    ).toDS()

    val m = Transformations.dailyMetrics(sessions, bookings, "2026-10-12").collect().map(r => r.customerId -> r).toMap

    m.keySet shouldBe Set("C1", "C2", "C3")

    m("C1").sessions shouldBe 2L
    m("C1").events shouldBe 8L
    m("C1").bookings shouldBe 1L
    m("C1").revenue shouldBe 100.0
    m("C1").conversionRate shouldBe 0.5

    m("C2").bookings shouldBe 0L
    m("C2").revenue shouldBe 0.0

    m("C3").sessions shouldBe 0L
    m("C3").bookings shouldBe 1L
    m("C3").conversionRate shouldBe 0.0

    m("C1").metricDate.toString shouldBe "2026-10-12"
  }

  test("dailyMetrics is deterministic: running it twice gives identical results") {
    val sessions = Seq(CustomerSession("C1", ts("2026-10-12 10:00:00"), ts("2026-10-12 10:30:00"), 1, 1, 0)).toDS()
    val bookings = Seq(booking("b1", "C1", 10.0, "2026-10-12 10:05:00")).toDS()
    val a = Transformations.dailyMetrics(sessions, bookings, "2026-10-12").collect().toSet
    val b = Transformations.dailyMetrics(sessions, bookings, "2026-10-12").collect().toSet
    a shouldBe b
  }

  test("freshness lag is measured in whole minutes") {
    val now = Instant.parse("2026-10-12T12:00:00Z")
    FreshnessCheckJob.lagMinutes(Instant.parse("2026-10-12T11:15:30Z"), now) shouldBe 44L
  }
}
