package sparkshield

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.slf4j.LoggerFactory

import java.io.File
import java.util.Properties

// ============================================================
// NetworkFlowProducer — Milestone 4
//
// Replays historical cleaned network flow records (CICIDS2017)
// as JSON messages to an Apache Kafka topic.
//
// NOTE: This is a streaming SIMULATION replaying historical
// flows. It does not perform live network packet capture.
//
// Features:
//   - Reads from existing cleaned Parquet output (data/processed).
//   - Generates stable event IDs (flow_id) and event timestamps.
//   - Serializes flow records into clean, validated JSON payloads.
//   - Supports bounded record counts and controllable delay (rate simulation).
//   - Partition-wise distributed streaming without collecting dataset to driver.
// ============================================================
object NetworkFlowProducer {

  private val log = LoggerFactory.getLogger(getClass)

  val DefaultTopic: String = NetworkFlowStreaming.DefaultTopic
  val DefaultBootstrapServers: String = NetworkFlowStreaming.DefaultBootstrapServers
  val DefaultInputPath: String = "data/processed/cleaned_2026-10-24"

  case class ProducerConfigSettings(
    inputPath       : String        = DefaultInputPath,
    bootstrapServers: String        = DefaultBootstrapServers,
    topic           : String        = DefaultTopic,
    maxRecords      : Option[Long]  = Some(5000L), // Default bounded sample for demo
    delayMs         : Long          = 2L,          // Controllable delay per event per partition
    stratified      : Boolean       = true         // Balanced selection of BENIGN and ATTACK records
  )

  /**
   * Enrich cleaned DataFrame with event identifier and event timestamp,
   * then serialize each flow record into a JSON payload with key and value.
   *
   * @param df Cleaned network flow DataFrame
   * @return DataFrame with columns: [key: String, value: String]
   */
  def prepareJsonDataFrame(df: DataFrame): DataFrame = {
    // Add stable event identifier and timestamp if not already present
    val withMeta = df
      .withColumn("flow_id", concat(lit("flow_"), monotonically_increasing_id().cast("string")))
      .withColumn("timestamp", current_timestamp().cast("string"))

    val structCols = withMeta.columns.map(col)

    withMeta.select(
      col("flow_id").cast("string").as("key"),
      to_json(struct(structCols: _*)).as("value")
    )
  }

  /**
   * Load cleaned Parquet data and prepare JSON DataFrame.
   * Supports stratified sampling to guarantee mixed BENIGN and ATTACK traffic in bounded samples.
   */
  def loadAndPrepare(
    inputPath : String,
    maxRecords: Option[Long],
    spark     : SparkSession,
    stratified: Boolean = true
  ): DataFrame = {
    val f = new File(inputPath)
    if (!f.exists()) {
      throw new IllegalArgumentException(s"Input Parquet dataset not found at: $inputPath")
    }

    log.info(s"Loading cleaned Parquet from: $inputPath")
    val rawDf = spark.read.parquet(inputPath)

    val boundedDf = (maxRecords, stratified) match {
      case (Some(limit), true) if limit > 0 =>
        val half = limit / 2
        val benignDf = rawDf.filter(upper(col("label_binary")) === "BENIGN").limit(half.toInt)
        val attackDf = rawDf.filter(upper(col("label_binary")) === "ATTACK").limit((limit - half).toInt)
        log.info(s"Selecting stratified sample of $limit records: $half BENIGN and ${limit - half} ATTACK flows")
        benignDf.union(attackDf)
      case (Some(limit), false) if limit > 0 =>
        log.info(s"Bounding replay dataset to sequential $limit records")
        rawDf.limit(limit.toInt)
      case _ =>
        log.info("Processing all available records in Parquet dataset")
        rawDf
    }

    prepareJsonDataFrame(boundedDf)
  }

  /**
   * Publish records to Kafka with controllable rate delay per event.
   * Distributed across Spark executors via foreachPartition.
   * Driver memory remains bounded.
   *
   * @return Total number of records published
   */
  val DefaultMaxPayloadBytes: Long = NetworkFlowStreaming.DefaultMaxPayloadBytes // 64 KB configurable max Kafka payload limit (SEC-05)

  /**
   * Filter DataFrame to only records whose UTF-8 JSON payload does not exceed maxPayloadBytes.
   * Evaluated in distributed Spark executors without collecting to driver memory.
   */
  def filterValidPayloads(
    jsonDf         : DataFrame,
    maxPayloadBytes: Long = DefaultMaxPayloadBytes
  ): DataFrame = {
    import org.apache.spark.sql.types.BinaryType
    jsonDf.filter(length(col("value").cast(BinaryType)) <= maxPayloadBytes)
  }

  /**
   * Publish records to Kafka with controllable rate delay per event.
   * Distributed across Spark executors via foreachPartition.
   * Enforces 65,536-byte UTF-8 payload limit per message (SEC-05).
   * Driver memory remains bounded.
   *
   * @return Total number of records published
   */
  def publishStreamingReplay(
    jsonDf          : DataFrame,
    bootstrapServers: String,
    topic           : String,
    delayMs         : Long,
    spark           : SparkSession,
    maxPayloadBytes : Long = DefaultMaxPayloadBytes
  ): Long = {
    val countAccum = spark.sparkContext.longAccumulator("published_records_counter")

    jsonDf.foreachPartition { rows: Iterator[Row] =>
      val props = new Properties()
      props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
      props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
      props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
      props.put(ProducerConfig.ACKS_CONFIG, "1")
      props.put(ProducerConfig.RETRIES_CONFIG, "3")
      props.put(ProducerConfig.LINGER_MS_CONFIG, "10")
      props.put(ProducerConfig.BATCH_SIZE_CONFIG, "16384")

      val producer = new KafkaProducer[String, String](props)
      try {
        var localCount = 0L
        while (rows.hasNext) {
          val row = rows.next()
          val key = row.getAs[String]("key")
          val value = row.getAs[String]("value")

          val payloadBytes = if (value != null) value.getBytes(java.nio.charset.StandardCharsets.UTF_8).length else 0
          if (payloadBytes > maxPayloadBytes) {
            org.slf4j.LoggerFactory.getLogger(getClass).warn(
              s"Rejecting oversized message (size: $payloadBytes bytes > limit: $maxPayloadBytes bytes) for key: $key"
            )
          } else {
            val record = new ProducerRecord[String, String](topic, key, value)
            producer.send(record)
            localCount += 1

            if (delayMs > 0) {
              Thread.sleep(delayMs)
            }
          }
        }
        producer.flush()
        countAccum.add(localCount)
      } finally {
        producer.close()
      }
    }

    countAccum.value
  }

  /**
   * High-throughput batch publish to Kafka using Spark's native Kafka DataFrame writer.
   * Filters out payloads exceeding maxPayloadBytes (SEC-05).
   */
  def publishBatch(
    jsonDf          : DataFrame,
    bootstrapServers: String,
    topic           : String,
    maxPayloadBytes : Long = DefaultMaxPayloadBytes
  ): Long = {
    import org.apache.spark.sql.types.BinaryType
    val validPayloads = jsonDf.filter(length(col("value").cast(BinaryType)) <= maxPayloadBytes)
    val totalRecords = validPayloads.count()
    validPayloads.write
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option("topic", topic)
      .save()
    totalRecords
  }

  // ── CLI Parser ──────────────────────────────────────────────

  def parseArgs(args: Array[String]): ProducerConfigSettings = {
    var config = ProducerConfigSettings()
    var i = 0
    while (i < args.length) {
      args(i) match {
        case "--input" if i + 1 < args.length =>
          config = config.copy(inputPath = args(i + 1))
          i += 2
        case "--servers" if i + 1 < args.length =>
          config = config.copy(bootstrapServers = args(i + 1))
          i += 2
        case "--topic" if i + 1 < args.length =>
          config = config.copy(topic = args(i + 1))
          i += 2
        case "--max-records" if i + 1 < args.length =>
          val countStr = args(i + 1)
          val count = if (countStr.equalsIgnoreCase("all")) None else Some(countStr.toLong)
          config = config.copy(maxRecords = count)
          i += 2
        case "--delay-ms" if i + 1 < args.length =>
          config = config.copy(delayMs = args(i + 1).toLong)
          i += 2
        case "--stratified" if i + 1 < args.length =>
          config = config.copy(stratified = args(i + 1).toBoolean)
          i += 2
        case "--sequential" =>
          config = config.copy(stratified = false)
          i += 1
        case other =>
          log.warn(s"Unrecognized argument: $other")
          i += 1
      }
    }
    config
  }

  def main(args: Array[String]): Unit = {
    val config = parseArgs(args)

    println("\n" + "=" * 75)
    println("  SparkShield — Milestone 4: Historical Network Flow Kafka Producer")
    println("  Mode: Streaming Replay Simulation (Historical Data)")
    println(s"  Input Parquet     : ${config.inputPath}")
    println(s"  Kafka Brokers     : ${config.bootstrapServers}")
    println(s"  Target Topic      : ${config.topic}")
    println(s"  Max Records       : ${config.maxRecords.map(_.toString).getOrElse("ALL")}")
    println(s"  Sample Stratified : ${config.stratified}")
    println(s"  Rate Delay (ms)   : ${config.delayMs} ms/record")
    println("=" * 75)

    val spark = SparkSessionFactory.create(
      "SparkShield-NetworkFlowProducer",
      extras = Map("spark.sql.shuffle.partitions" -> "4")
    )

    try {
      val startTime = System.currentTimeMillis()

      val jsonDf = loadAndPrepare(config.inputPath, config.maxRecords, spark, config.stratified)

      val published = if (config.delayMs > 0) {
        println(s"\nPublishing events with simulated streaming delay (${config.delayMs}ms)...")
        publishStreamingReplay(jsonDf, config.bootstrapServers, config.topic, config.delayMs, spark)
      } else {
        println("\nPublishing events in high-throughput batch mode...")
        publishBatch(jsonDf, config.bootstrapServers, config.topic)
      }

      val elapsedSeconds = math.max((System.currentTimeMillis() - startTime) / 1000.0, 0.001)
      val throughput = published / elapsedSeconds

      println("\n" + "=" * 75)
      println("  Publication Summary:")
      println(f"  Total Published   : $published%,d events")
      println(f"  Elapsed Time      : $elapsedSeconds%.2f seconds")
      println(f"  Replay Throughput : $throughput%.2f events/sec")
      println("  NetworkFlowProducer — COMPLETE")
      println("=" * 75 + "\n")
    } finally {
      spark.stop()
    }
  }
}
