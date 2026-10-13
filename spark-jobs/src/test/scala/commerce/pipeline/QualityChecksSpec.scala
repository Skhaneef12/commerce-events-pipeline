package commerce.pipeline

import com.amazon.deequ.VerificationSuite
import com.amazon.deequ.checks.CheckStatus
import commerce.pipeline.jobs.QualityJob
import commerce.pipeline.model.DailyCustomerMetric
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.sql.Date

class QualityChecksSpec extends AnyFunSuite with Matchers with SparkTestSession {
  import spark.implicits._

  private val d = Date.valueOf("2026-10-12")

  private def run(rows: Seq[DailyCustomerMetric]) =
    VerificationSuite().onData(rows.toDF()).addCheck(QualityJob.checks).run().status

  test("a clean partition passes the quality gate") {
    run(Seq(
      DailyCustomerMetric(d, "C1", 2, 8, 1, 100.0, 0.5),
      DailyCustomerMetric(d, "C2", 1, 2, 0, 0.0, 0.0)
    )) shouldBe CheckStatus.Success
  }

  test("a duplicate customerId fails the primary-key check") {
    run(Seq(
      DailyCustomerMetric(d, "C1", 2, 8, 1, 100.0, 0.5),
      DailyCustomerMetric(d, "C1", 1, 2, 0, 0.0, 0.0)
    )) shouldBe CheckStatus.Error
  }

  test("negative revenue fails") {
    run(Seq(DailyCustomerMetric(d, "C1", 1, 1, 1, -5.0, 1.0))) shouldBe CheckStatus.Error
  }

  test("too many bookings without sessions breaches the 99% threshold") {
    val rows = (1 to 98).map(i => DailyCustomerMetric(d, s"C$i", 1, 1, 0, 0.0, 0.0)) ++
      Seq(DailyCustomerMetric(d, "X1", 0, 0, 1, 10.0, 0.0), DailyCustomerMetric(d, "X2", 0, 0, 1, 10.0, 0.0))
    run(rows) shouldBe CheckStatus.Error // 98% < 99%
  }
}
