package commerce.pipeline.config

/** All runtime settings, read from environment variables so the same jar runs
  * locally, in Docker, and in Airflow without code changes (12-factor style).
  */
final case class PipelineConfig(
    kafkaBootstrap: String,
    clickTopic: String,
    bookingTopic: String,
    dataRoot: String,
    sessionGap: String,
    watermark: String,
    triggerInterval: String,
    mongoEnabled: Boolean,
    mongoUri: String,
    mongoDatabase: String,
    mongoCollection: String,
    freshnessSloMinutes: Long
) {
  def sessionsPath: String   = s"$dataRoot/silver/customer_sessions"
  def bookingsPath: String   = s"$dataRoot/silver/bookings"
  def metricsPath: String    = s"$dataRoot/gold/daily_customer_metrics"
  def checkpointRoot: String = s"$dataRoot/_checkpoints"
}

object PipelineConfig {
  private def env(key: String, default: String): String = sys.env.getOrElse(key, default)

  def fromEnv(): PipelineConfig = PipelineConfig(
    kafkaBootstrap      = env("KAFKA_BOOTSTRAP", "localhost:29092"),
    clickTopic          = env("CLICK_TOPIC", "clickstream.events"),
    bookingTopic        = env("BOOKING_TOPIC", "booking.events"),
    dataRoot            = env("DATA_ROOT", "../data"),
    sessionGap          = env("SESSION_GAP", "30 minutes"),
    watermark           = env("WATERMARK", "10 minutes"),
    triggerInterval     = env("TRIGGER_INTERVAL", "30 seconds"),
    mongoEnabled        = env("MONGO_ENABLED", "false").toBoolean,
    mongoUri            = env("MONGO_URI", "mongodb://localhost:27017"),
    mongoDatabase       = env("MONGO_DATABASE", "commerce"),
    mongoCollection     = env("MONGO_COLLECTION", "daily_customer_metrics"),
    freshnessSloMinutes = env("FRESHNESS_SLO_MINUTES", "60").toLong
  )
}
