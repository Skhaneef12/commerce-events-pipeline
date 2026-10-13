package commerce.pipeline.jobs

import commerce.pipeline.common.SparkSessionBuilder
import commerce.pipeline.config.PipelineConfig
import commerce.pipeline.model.{BookingEvent, CustomerSession}
import commerce.pipeline.transform.Transformations
import org.apache.spark.sql.{SaveMode, SparkSession}

/** Silver -> Gold daily customer metrics for one date.
  *
  * Usage: spark-submit --class commerce.pipeline.jobs.DailyMetricsJob <jar> 2026-10-12
  *
  * Idempotent: Delta `replaceWhere` overwrites only that date's partition, and the
  * MongoDB write upserts on (metricDate, customerId). Re-running a day, through an
  * Airflow retry or a backfill, never creates duplicates.
  */
object DailyMetricsJob {

  def main(args: Array[String]): Unit = {
    require(args.length == 1, "usage: DailyMetricsJob <yyyy-MM-dd>")
    val date = args(0)
    require(date.matches("""\d{4}-\d{2}-\d{2}"""), s"bad date: $date")

    val cfg                         = PipelineConfig.fromEnv()
    implicit val spark: SparkSession = SparkSessionBuilder.build(s"daily-metrics-$date")
    import spark.implicits._

    val sessions = spark.read.format("delta").load(cfg.sessionsPath).as[CustomerSession]
    val bookings = spark.read.format("delta").load(cfg.bookingsPath).as[BookingEvent]

    val metrics = Transformations.dailyMetrics(sessions, bookings, date).cache()
    val rows    = metrics.count()

    metrics.write
      .format("delta")
      .mode(SaveMode.Overwrite)
      .partitionBy("metricDate")
      .option("replaceWhere", s"metricDate = '$date'")
      .save(cfg.metricsPath)

    if (cfg.mongoEnabled) {
      metrics.write
        .format("mongodb")
        .mode(SaveMode.Append)
        .option("connection.uri", cfg.mongoUri)
        .option("database", cfg.mongoDatabase)
        .option("collection", cfg.mongoCollection)
        .option("operationType", "replace")
        .option("idFieldList", "metricDate,customerId")
        .option("upsertDocument", "true")
        .save()
    }

    println(s"[daily-metrics] date=$date rows=$rows mongo=${cfg.mongoEnabled}")
    spark.stop()
  }
}
