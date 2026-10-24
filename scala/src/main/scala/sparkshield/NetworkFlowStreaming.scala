package sparkshield

import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, Trigger}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.slf4j.LoggerFactory

import java.io.File

// ============================================================
// NetworkFlowStreaming — Milestone 4
//
// Consumes network flow events from Apache Kafka using Spark
// Structured Streaming. Parses JSON payloads, validates schemas,
// handles malformed messages, computes windowed traffic & attack
// aggregations with watermarks, and writes streaming results.
//
// NOTE: This streaming application processes simulated flow
// records replayed from historical datasets.
// ============================================================
object NetworkFlowStreaming {

  private val log = LoggerFactory.getLogger(getClass)

  val DefaultTopic: String = "network-flows"
  val DefaultBootstrapServers: String = "localhost:9092"
  val DefaultCheckpointPath: String = "checkpoints/network-flows-streaming"
  val DefaultOutputPath: String = "results/streaming"
  val DefaultMaxPayloadBytes: Long = 65536L // 64 KB configurable max Kafka payload limit (SEC-05)

  // ── 1. Explicit JSON Event Schema ──────────────────────────

  val FlowEventSchema: StructType = StructType(Seq(
    StructField("flow_id", StringType, nullable = true),
    StructField("timestamp", StringType, nullable = true),
    StructField("destination_port", DoubleType, nullable = true),
    StructField("flow_duration", DoubleType, nullable = true),
    StructField("total_fwd_packets", DoubleType, nullable = true),
    StructField("total_backward_packets", DoubleType, nullable = true),
    StructField("total_length_of_fwd_packets", DoubleType, nullable = true),
    StructField("total_length_of_bwd_packets", DoubleType, nullable = true),
    StructField("flow_bytes_s", DoubleType, nullable = true),
    StructField("flow_packets_s", DoubleType, nullable = true),
    StructField("fwd_packet_length_max", DoubleType, nullable = true),
    StructField("bwd_packet_length_max", DoubleType, nullable = true),
    StructField("fwd_header_length", DoubleType, nullable = true),
    StructField("fwd_header_length_2", DoubleType, nullable = true),
    StructField("has_invalid_numeric", BooleanType, nullable = true),
    StructField("label_original", StringType, nullable = true),
    StructField("label_binary", StringType, nullable = true)
  ))

  // ── 2. Stream Ingestion & Parsing ──────────────────────────

  /**
   * Identifies if a numeric column value is non-finite (NaN, +Infinity, -Infinity).
   * Returns false if the value is null.
   */
  def isNonFinite(c: Column): Column = DataCleaning.isNonFinite(c)

  /**
   * Sanitizes a numeric column for statistical aggregation.
   * If non-finite (NaN, +/-Infinity) or null, returns NULL so Spark SQL aggregation functions
   * (such as avg, stddev) safely ignore it without corrupting results to Infinity or NaN.
   * Does NOT replace invalid values with arbitrary numbers (such as 0.0).
   */
  def finiteNumeric(c: Column): Column =
    when(c.isNotNull && !c.isNaN && c =!= Double.PositiveInfinity && c =!= Double.NegativeInfinity, c)
      .otherwise(lit(null).cast(DoubleType))

  /**
   * Create raw Kafka streaming DataFrame.
   */
  def readKafkaStream(
    spark           : SparkSession,
    bootstrapServers: String = DefaultBootstrapServers,
    topic           : String = DefaultTopic,
    startingOffsets : String = "earliest"
  ): DataFrame = {
    log.info(s"Connecting Structured Streaming to Kafka: $bootstrapServers, topic: $topic")
    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option("subscribe", topic)
      .option("startingOffsets", startingOffsets)
      .option("failOnDataLoss", "false")
      .load()
  }

  /**
   * Parse raw Kafka messages into structured, typed event stream.
   * Handles malformed JSON payloads and oversized payloads gracefully.
   *
   * SEC-05: Enforces configurable payload size limit (default: 65,536 bytes)
   * against actual UTF-8 payload bytes (BinaryType cast), not character length.
   * Oversized messages are flagged (is_oversized = true, is_valid = false)
   * and can be routed to dead-letter sinks.
   *
   * Adds:
   *   - payload_bytes: Long (actual UTF-8 byte length of raw payload)
   *   - is_oversized: Boolean (true if payload_bytes > maxPayloadBytes)
   *   - is_valid: Boolean (true if parsed cleanly, within size limit, and required fields exist)
   *   - is_malformed: Boolean (true if parsing failed, oversized, or required fields missing)
   *   - is_invalid_numeric: Boolean (true if numeric features contain NaN or Infinity)
   *   - event_time: TimestampType (for event-time windowing)
   */
  def parseAndValidateStream(
    rawStream      : DataFrame,
    schema         : StructType = FlowEventSchema,
    maxPayloadBytes: Long       = DefaultMaxPayloadBytes
  ): DataFrame = {
    val valueCol = if (rawStream.columns.contains("value")) col("value") else col("raw_payload")
    val stringValCol = valueCol.cast(StringType)
    val binaryValCol = valueCol.cast(BinaryType)

    val withParsed = rawStream
      .select(
        (if (rawStream.columns.contains("key")) col("key").cast("string") else lit(null).cast("string")).as("kafka_key"),
        stringValCol.as("raw_payload"),
        coalesce(length(binaryValCol), lit(0)).cast(LongType).as("payload_bytes"),
        from_json(stringValCol, schema).as("event")
      )

    val isOversized = col("payload_bytes") > maxPayloadBytes

    val hasNonFinite = coalesce(
      (col("event.has_invalid_numeric") === true) ||
      isNonFinite(col("event.destination_port")) ||
      isNonFinite(col("event.flow_duration")) ||
      isNonFinite(col("event.total_fwd_packets")) ||
      isNonFinite(col("event.total_backward_packets")) ||
      isNonFinite(col("event.flow_bytes_s")) ||
      isNonFinite(col("event.flow_packets_s")) ||
      isNonFinite(col("event.fwd_packet_length_max")) ||
      isNonFinite(col("event.bwd_packet_length_max")) ||
      isNonFinite(col("event.fwd_header_length")) ||
      isNonFinite(col("event.fwd_header_length_2")),
      lit(false)
    )

    // Normalize binary label: BENIGN -> BENIGN, recognized attacks -> ATTACK, else UNKNOWN
    val rawBinaryLabel = col("event.label_binary")
    val rawOrigLabel = col("event.label_original")
    val normalizedBinary = when(rawBinaryLabel.isNull || length(trim(rawBinaryLabel)) === 0, lit("UNKNOWN"))
      .when(upper(trim(rawBinaryLabel)) === "BENIGN", lit("BENIGN"))
      .when(upper(trim(rawBinaryLabel)) === "ATTACK", lit("ATTACK"))
      .when(DataCleaning.isRecognizedAttack(coalesce(rawOrigLabel, rawBinaryLabel)), lit("ATTACK"))
      .otherwise(lit("UNKNOWN"))

    val isValid =
      !isOversized &&
      col("event").isNotNull &&
      col("event.flow_id").isNotNull &&
      col("event.timestamp").isNotNull &&
      rawBinaryLabel.isNotNull &&
      length(trim(rawBinaryLabel)) > 0 &&
      rawOrigLabel.isNotNull &&
      to_timestamp(col("event.timestamp")).isNotNull

    val isMalformed = !isValid

    withParsed.select(
      col("kafka_key"),
      col("raw_payload"),
      col("payload_bytes"),
      isOversized.as("is_oversized"),
      col("event.flow_id"),
      col("event.timestamp"),
      col("event.destination_port"),
      col("event.flow_duration"),
      col("event.total_fwd_packets"),
      col("event.total_backward_packets"),
      col("event.flow_bytes_s"),
      col("event.flow_packets_s"),
      col("event.fwd_packet_length_max"),
      col("event.bwd_packet_length_max"),
      col("event.fwd_header_length"),
      col("event.fwd_header_length_2"),
      hasNonFinite.as("has_invalid_numeric"),
      col("event.label_original"),
      normalizedBinary.as("label_binary"),
      isValid.as("is_valid"),
      isMalformed.as("is_malformed"),
      hasNonFinite.as("is_invalid_numeric"),
      to_timestamp(col("event.timestamp")).as("event_time")
    )
  }

  /**
   * Filter rejected or oversized events for dead-letter logging or dead-letter queue routing (SEC-05).
   */
  def deadLetterStream(streamDf: DataFrame): DataFrame = {
    streamDf.filter(col("is_valid") === false)
  }

  // ── 3. Streaming Aggregations ──────────────────────────────

  /**
   * Windowed traffic aggregations with watermarking for late-event handling.
   * Uses finiteNumeric to avoid Infinity/NaN corruption of mathematical averages.
   * Tracks invalid_numeric_count explicitly.
   */
  def computeWindowedTraffic(
    validStream   : DataFrame,
    watermarkDelay: String = "10 seconds",
    windowDuration: String = "1 minute",
    slideDuration : String = "30 seconds"
  ): DataFrame = {
    validStream
      .withWatermark("event_time", watermarkDelay)
      .groupBy(
        window(col("event_time"), windowDuration, slideDuration),
        col("label_binary")
      )
      .agg(
        count(lit(1)).as("flow_count"),
        sum(when(col("has_invalid_numeric") === true, 1).otherwise(0)).as("invalid_numeric_count"),
        round(avg(finiteNumeric(col("flow_duration"))), 4).as("avg_duration"),
        round(avg(finiteNumeric(col("flow_bytes_s"))), 4).as("avg_bytes_s"),
        round(avg(finiteNumeric(col("flow_packets_s"))), 4).as("avg_packets_s")
      )
  }

  /**
   * Cumulative binary attack distribution (BENIGN vs ATTACK vs UNKNOWN).
   * Intended for OutputMode.Complete or foreachBatch.
   */
  def computeBinarySummary(validStream: DataFrame): DataFrame = {
    validStream
      .groupBy(col("label_binary"))
      .agg(
        count(lit(1)).as("flow_count"),
        sum(when(col("has_invalid_numeric") === true, 1).otherwise(0)).as("invalid_numeric_count"),
        round(avg(finiteNumeric(col("flow_duration"))), 4).as("avg_duration"),
        round(avg(finiteNumeric(col("flow_bytes_s"))), 4).as("avg_bytes_s")
      )
  }

  /**
   * Comprehensive stream quality and event categorization audit.
   * Accurately distinguishes total events, valid flows, malformed JSON,
   * oversized payloads (SEC-05), invalid numeric records, benign vs attack classification,
   * and unknown labels.
   */
  def computeQualitySummary(streamDf: DataFrame): DataFrame = {
    streamDf.agg(
      count(lit(1)).as("total_events"),
      sum(when(col("is_valid") === true, 1).otherwise(0)).as("valid_count"),
      sum(when(col("is_malformed") === true, 1).otherwise(0)).as("malformed_count"),
      sum(when(col("is_oversized") === true, 1).otherwise(0)).as("oversized_count"),
      sum(when(col("is_valid") === true && col("has_invalid_numeric") === true, 1).otherwise(0)).as("invalid_numeric_count"),
      sum(when(col("is_valid") === true && upper(col("label_binary")) === "BENIGN", 1).otherwise(0)).as("benign_count"),
      sum(when(col("is_valid") === true && upper(col("label_binary")) === "ATTACK", 1).otherwise(0)).as("attack_count"),
      sum(when(col("is_valid") === true && (col("label_binary").isNull || upper(col("label_binary")) === "UNKNOWN"), 1).otherwise(0)).as("unknown_label_count")
    )
  }

  /**
   * Attack counts grouped by original label (DDoS, PortScan, etc.).
   */
  def computeAttackLabelSummary(validStream: DataFrame): DataFrame = {
    validStream
      .filter(upper(col("label_binary")) === "ATTACK")
      .groupBy(col("label_original"))
      .agg(
        count(lit(1)).as("attack_count")
      )
  }

  // ── 4. Micro-batch Persistence (foreachBatch) ──────────────

  /**
   * Safe micro-batch processor writing streaming batches to Parquet and CSV.
   * Gracefully handles empty micro-batches without writing redundant empty data.
   * Idempotent under batch retries via directory overwrite mode.
   */
  def processMicroBatch(
    batchDf  : DataFrame,
    batchId  : Long,
    outputDir: String
  ): Unit = {
    if (batchDf.isEmpty) {
      log.debug(s"Micro-batch $batchId is empty. Skipping persistence.")
      return
    }

    val totalCount = batchDf.count()
    val benignCount = batchDf.filter(upper(col("label_binary")) === "BENIGN").count()
    val attackCount = batchDf.filter(upper(col("label_binary")) === "ATTACK").count()
    val invalidNumericCount = batchDf.filter(col("has_invalid_numeric") === true).count()

    log.info(s"Processing micro-batch $batchId: total=$totalCount, benign=$benignCount, attack=$attackCount, invalid_numeric=$invalidNumericCount")
    println(s"[Micro-Batch $batchId] Persisting $totalCount records (BENIGN: $benignCount, ATTACK: $attackCount, Invalid Numerics: $invalidNumericCount)")

    val cleanBatch = if (batchDf.columns.contains("raw_payload")) {
      batchDf.drop("raw_payload", "kafka_key")
    } else batchDf

    val batchParquet = s"$outputDir/parquet/batch_$batchId"
    cleanBatch.write
      .mode("overwrite")
      .parquet(batchParquet)

    val batchCsv = s"$outputDir/csv/batch_$batchId"
    cleanBatch.coalesce(1).write
      .option("header", "true")
      .mode("overwrite")
      .csv(batchCsv)
  }

  // ── 5. Stream Sinks ────────────────────────────────────────

  /**
   * Start a streaming query writing windowed traffic aggregations to console.
   */
  def startConsoleWindowQuery(
    windowedDf    : DataFrame,
    checkpointPath: String
  ): StreamingQuery = {
    new File(checkpointPath).mkdirs()
    windowedDf.writeStream
      .outputMode(OutputMode.Update())
      .format("console")
      .option("checkpointLocation", checkpointPath)
      .option("truncate", "false")
      .trigger(Trigger.ProcessingTime("2 seconds"))
      .start()
  }

  /**
   * Start a memory sink streaming query (useful for unit tests and verification).
   */
  def startMemoryQuery(
    df       : DataFrame,
    queryName: String
  ): StreamingQuery = {
    df.writeStream
      .outputMode(OutputMode.Complete())
      .format("memory")
      .queryName(queryName)
      .start()
  }

  // ── 6. CLI Entry Point ─────────────────────────────────────

  case class StreamingConfig(
    bootstrapServers: String = DefaultBootstrapServers,
    topic           : String = DefaultTopic,
    checkpointPath  : String = DefaultCheckpointPath,
    outputPath      : String = DefaultOutputPath,
    durationSeconds : Long   = 20L // Default duration for non-interactive execution
  )

  def parseArgs(args: Array[String]): StreamingConfig = {
    var config = StreamingConfig()
    var i = 0
    while (i < args.length) {
      args(i) match {
        case "--servers" if i + 1 < args.length =>
          config = config.copy(bootstrapServers = args(i + 1))
          i += 2
        case "--topic" if i + 1 < args.length =>
          config = config.copy(topic = args(i + 1))
          i += 2
        case "--checkpoint" if i + 1 < args.length =>
          config = config.copy(checkpointPath = args(i + 1))
          i += 2
        case "--output" if i + 1 < args.length =>
          config = config.copy(outputPath = args(i + 1))
          i += 2
        case "--duration-seconds" if i + 1 < args.length =>
          config = config.copy(durationSeconds = args(i + 1).toLong)
          i += 2
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
    println("  SparkShield — Milestone 4: Spark Structured Streaming Consumer")
    println("  Mode: Historical Event Replay Streaming Analytics")
    println(s"  Kafka Brokers     : ${config.bootstrapServers}")
    println(s"  Subscribed Topic  : ${config.topic}")
    println(s"  Checkpoint Dir    : ${config.checkpointPath}")
    println(s"  Output Path       : ${config.outputPath}")
    println(s"  Execution Timeout : ${config.durationSeconds} seconds")
    println("=" * 75)

    val spark = SparkSessionFactory.create(
      "SparkShield-NetworkFlowStreaming",
      extras = Map("spark.sql.shuffle.partitions" -> "4")
    )

    try {
      // 1. Ingest raw stream from Kafka
      val rawStream = readKafkaStream(spark, config.bootstrapServers, config.topic)

      // 2. Parse JSON and validate records
      val parsedStream = parseAndValidateStream(rawStream)
      val validFlows = parsedStream.filter(col("is_valid") === true)

      // 3. Windowed aggregation query (1-minute window, 30s slide, 10s watermark)
      val windowedTraffic = computeWindowedTraffic(validFlows, "10 seconds", "1 minute", "30 seconds")

      // 4. Start Windowed Console Query
      val windowQuery = windowedTraffic.writeStream
        .outputMode(OutputMode.Update())
        .format("console")
        .option("checkpointLocation", s"${config.checkpointPath}/window")
        .option("truncate", "false")
        .trigger(Trigger.ProcessingTime("2 seconds"))
        .start()

      // 5. Start Persistent ForeachBatch Query (writing to Parquet and CSV)
      val batchQuery = validFlows.writeStream
        .foreachBatch { (batchDf: DataFrame, batchId: Long) =>
          processMicroBatch(batchDf, batchId, config.outputPath)
        }
        .option("checkpointLocation", s"${config.checkpointPath}/batch")
        .trigger(Trigger.ProcessingTime("2 seconds"))
        .start()

      println("\nStreaming queries started successfully. Awaiting data...")

      if (config.durationSeconds > 0) {
        println(s"Running stream for ${config.durationSeconds} seconds...")
        windowQuery.awaitTermination(config.durationSeconds * 1000L)
        batchQuery.awaitTermination(config.durationSeconds * 1000L)
        println("\nTimeout reached. Gracefully stopping streaming queries...")
        windowQuery.stop()
        batchQuery.stop()
      } else {
        println("Running stream continuously. Press Ctrl+C to stop.")
        spark.streams.awaitAnyTermination()
      }

      println("\n" + "=" * 75)
      println("  Spark Structured Streaming Consumer — COMPLETE")
      println("=" * 75 + "\n")
    } finally {
      spark.stop()
    }
  }
}
