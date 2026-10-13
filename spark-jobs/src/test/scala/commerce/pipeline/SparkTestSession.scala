package commerce.pipeline

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Suite}

/** One small local SparkSession shared by a test suite. */
trait SparkTestSession extends BeforeAndAfterAll { self: Suite =>

  implicit lazy val spark: SparkSession = SparkSession
    .builder()
    .master("local[2]")
    .appName("unit-tests")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.shuffle.partitions", "2")
    .config("spark.ui.enabled", "false")
    .getOrCreate()

  override def afterAll(): Unit = {
    spark.stop()
    super.afterAll()
  }
}
