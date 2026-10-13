package commerce.pipeline.jobs

import commerce.pipeline.common.SparkSessionBuilder
import commerce.pipeline.config.PipelineConfig
import commerce.pipeline.transform.Transformations
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.Trigger

/** Kafka -> Delta (silver), running continuously.
  *
  * Two streaming queries:
  *   1. clickstream.events -> sessionized customer sessions
  *   2. booking.events     -> deduplicated bookings
  *
  * Delivery guarantee: each query has its own checkpoint (Kafka offsets + state),
  * and the Delta sink commits each micro-batch as a transaction. Together that
  * gives end-to-end exactly-once into Delta, even after a crash and restart.
  */
object StreamingIngestJob {

  def main(args: Array[String]): Unit = {
    val cfg                         = PipelineConfig.fromEnv()
    implicit val spark: SparkSession = SparkSessionBuilder.build("streaming-ingest")

    def readTopic(topic: String): DataFrame =
      spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", cfg.kafkaBootstrap)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", "50000") // back-pressure: cap each micro-batch
        .option("failOnDataLoss", "false")
        .load()

    val sessions = Transformations.sessionize(
      Transformations.parseClicks(readTopic(cfg.clickTopic)),
      cfg.sessionGap,
      cfg.watermark
    )

    // dropDuplicatesWithinWatermark (Spark 3.5+) keeps dedup state bounded by the watermark.
    val bookings = Transformations
      .parseBookings(readTopic(cfg.bookingTopic))
      .withWatermark("eventTime", cfg.watermark)
      .dropDuplicatesWithinWatermark(Seq("bookingId"))

    sessions.writeStream
      .queryName("customer_sessions")
      .format("delta")
      .outputMode("append") // a session is emitted once the watermark passes its end
      .option("checkpointLocation", s"${cfg.checkpointRoot}/customer_sessions")
      .trigger(Trigger.ProcessingTime(cfg.triggerInterval))
      .start(cfg.sessionsPath)

    bookings.writeStream
      .queryName("bookings")
      .format("delta")
      .outputMode("append")
      .option("checkpointLocation", s"${cfg.checkpointRoot}/bookings")
      .trigger(Trigger.ProcessingTime(cfg.triggerInterval))
      .start(cfg.bookingsPath)

    spark.streams.awaitAnyTermination()
  }
}
