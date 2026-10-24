package sparkshield

import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, Trigger}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}
import org.slf4j.LoggerFactory

import java.io.{File, PrintWriter}
import java.time.Instant
import java.util.concurrent.atomic.AtomicLong

/**
 * ============================================================
 * NetworkFlowMLStreaming — Milestone 6
 *
 * Real-time network intrusion detection using Spark Structured Streaming
 * and a pre-trained Spark MLlib PipelineModel.
 *
 * Data flow:
 *   Kafka Topic -> Spark Structured Streaming -> JSON Validation ->
 *   Feature Extraction -> Pre-trained MLlib PipelineModel ->
 *   Prediction & Probability Generation -> Idempotent Persistence (Parquet & CSV)
 *
 * Distinguishes valid events from malformed/invalid events without
 * misclassifying prediction failures as BENIGN.
 * ============================================================
 */
object NetworkFlowMLStreaming {

  private val log = LoggerFactory.getLogger(getClass)

  val DefaultTopic: String = NetworkFlowStreaming.DefaultTopic
  val DefaultBootstrapServers: String = NetworkFlowStreaming.DefaultBootstrapServers
  val DefaultCheckpointPath: String = "checkpoints/network-flows-ml-streaming"
  val DefaultOutputPath: String = "results/streaming-ml"
  val DefaultModelPath: String = "results/ml/run-20261024-115612/model"
  val DefaultMaxPayloadBytes: Long = NetworkFlowStreaming.DefaultMaxPayloadBytes // 64 KB (SEC-05)

  case class MLStreamConfig(
    topic           : String       = DefaultTopic,
    bootstrapServers: String       = DefaultBootstrapServers,
    checkpointPath  : String       = DefaultCheckpointPath,
    outputPath      : String       = DefaultOutputPath,
    modelPath       : String       = DefaultModelPath,
    durationSeconds : Option[Long] = None,
    maxPayloadBytes : Long         = DefaultMaxPayloadBytes
  )

  case class StreamMetrics(
    recordsConsumed : Long,
    recordsValid    : Long,
    recordsRejected : Long,
    predictedBenign : Long,
    predictedAttack : Long,
    batchCount      : Long
  )

  // ── 1. Model Loading & Feature Contract ─────────────────────

  /**
   * Load the saved PipelineModel and dynamically extract its required feature names.
   *
   * @param modelPath Path to the saved PipelineModel directory
   * @return (PipelineModel, Array of expected feature names in order)
   */
  def loadModelAndContract(modelPath: String): (PipelineModel, Array[String]) = {
    val f = new File(modelPath)
    if (!f.exists()) {
      throw new IllegalArgumentException(s"Saved ML model not found at: $modelPath")
    }

    log.info(s"Loading saved Spark MLlib PipelineModel from: $modelPath")
    val model = PipelineModel.load(modelPath)

    val assemblerStage = model.stages.collectFirst { case va: VectorAssembler => va }
      .getOrElse(throw new IllegalStateException("PipelineModel does not contain a VectorAssembler stage."))

    val requiredFeatures = assemblerStage.getInputCols
    log.info(s"Model contract verified: requires ${requiredFeatures.length} features: ${requiredFeatures.take(5).mkString(", ")}...")

    (model, requiredFeatures)
  }

  /**
   * Construct the JSON event schema dynamically based on the model's required features
   * plus standard metadata fields.
   */
  def buildEventSchema(featureNames: Array[String]): StructType = {
    val metadataFields = Seq(
      StructField("flow_id", StringType, nullable = true),
      StructField("timestamp", StringType, nullable = true),
      StructField("label_original", StringType, nullable = true),
      StructField("label_binary", StringType, nullable = true),
      StructField("has_invalid_numeric", BooleanType, nullable = true)
    )

    val featureFields = featureNames.map(f => StructField(f, DoubleType, nullable = true))
    StructType(metadataFields ++ featureFields)
  }

  // UDF to validate that all features in a struct row are finite doubles without deep AST recursion
  val isNonFiniteUdf = udf((row: Row) => {
    if (row == null) true
    else {
      var i = 0
      var bad = false
      while (i < row.length && !bad) {
        if (row.isNullAt(i)) {
          bad = true
        } else {
          row.get(i) match {
            case d: java.lang.Double => if (d.isNaN || d.isInfinite) bad = true
            case n: java.lang.Number => if (n.doubleValue().isNaN || n.doubleValue().isInfinite) bad = true
            case _ => bad = true
          }
        }
        i += 1
      }
      bad
    }
  })

  /**
   * Parses raw Kafka streaming DataFrame, validates payload sizes, extracts fields,
   * and flags records that contain missing fields, malformed JSON, or non-finite numbers.
   */
  def parseAndValidateStream(
    rawStream      : DataFrame,
    schema         : StructType,
    featureNames   : Array[String],
    maxPayloadBytes: Long = DefaultMaxPayloadBytes
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

    // Non-finite check across all required feature columns using single struct UDF to prevent AST explosion
    val featureStruct = struct(featureNames.map(f => col(s"event.$f")): _*)
    val hasNonFinite = when(col("event").isNull, lit(true)).otherwise(
      coalesce(col("event.has_invalid_numeric") === true || isNonFiniteUdf(featureStruct), lit(false))
    )

    val isValid =
      !isOversized &&
      col("event").isNotNull &&
      col("event.flow_id").isNotNull &&
      !hasNonFinite

    val selectCols = Seq(
      col("kafka_key"),
      col("raw_payload"),
      col("payload_bytes"),
      isOversized.as("is_oversized"),
      isValid.as("is_valid"),
      hasNonFinite.as("has_invalid_numeric"),
      col("event.flow_id").as("flow_id"),
      col("event.timestamp").as("event_timestamp"),
      col("event.label_binary").as("ground_truth_label"),
      col("event.label_original").as("ground_truth_original")
    ) ++ featureNames.map(f => col(s"event.$f").as(f))

    withParsed.select(selectCols: _*)
  }

  // ── 3. Model Inference Helper ───────────────────────────────

  // UDFs to extract class probabilities from the Spark ML probability Vector
  val getBenignProb = udf((v: Vector) => if (v != null && v.size >= 1) v(0) else 0.0)
  val getAttackProb = udf((v: Vector) => if (v != null && v.size >= 2) v(1) else 0.0)

  /**
   * Applies the trained PipelineModel to valid records and formats the prediction output.
   */
  def applyInference(
    validDf     : DataFrame,
    model       : PipelineModel,
    modelVersion: String
  ): DataFrame = {
    if (validDf.isEmpty) {
      return validDf
    }

    val scored = model.transform(validDf)

    scored
      .withColumn("probability_benign", getBenignProb(col("probability")))
      .withColumn("probability_attack", getAttackProb(col("probability")))
      .withColumn("predicted_label_numeric", col("prediction"))
      .withColumn(
        "predicted_label",
        when(col("prediction") === 1.0, "ATTACK").otherwise("BENIGN")
      )
      .withColumn("model_version", lit(modelVersion))
      .withColumn("inference_status", lit("SUCCESS"))
      .withColumn("processing_time", current_timestamp().cast(StringType))
      .withColumn(
        "is_correct",
        when(col("ground_truth_label").isNotNull, col("predicted_label") === col("ground_truth_label"))
          .otherwise(lit(null).cast(BooleanType))
      )
      .drop("raw_features", "features", "rawPrediction", "probability", "prediction")
  }

  // ── 4. Idempotent Micro-Batch Persistence ───────────────────

  /**
   * Process a single micro-batch:
   *   1. Separates valid records from invalid/malformed records.
   *   2. Executes model inference on valid records.
   *   3. Formats rejected records with inference_status and predicted_label="REJECTED".
   *   4. Idempotently writes predictions to Parquet and CSV.
   *   5. Updates cumulative atomic performance counters.
   */
  def processMicroBatch(
    batchDf        : DataFrame,
    batchId        : Long,
    model          : PipelineModel,
    modelVersion   : String,
    outputPath     : String,
    totalConsumed  : AtomicLong,
    totalValid     : AtomicLong,
    totalRejected  : AtomicLong,
    totalBenign    : AtomicLong,
    totalAttack    : AtomicLong
  ): Unit = {
    if (batchDf.isEmpty) {
      log.debug(s"[Micro-Batch $batchId] Empty batch received; skipping persistence.")
      return
    }

    val t0 = System.currentTimeMillis()
    val batchCount = batchDf.count()
    totalConsumed.addAndGet(batchCount)

    val validDf = batchDf.filter(col("is_valid") === true).cache()
    val invalidDf = batchDf.filter(col("is_valid") === false).cache()

    val validCount = validDf.count()
    val invalidCount = invalidDf.count()

    totalValid.addAndGet(validCount)
    totalRejected.addAndGet(invalidCount)

    // 1. Run inference on valid records
    val predictionsDf = if (validCount > 0) {
      applyInference(validDf, model, modelVersion).cache()
    } else {
      null
    }

    // Tally predicted labels
    if (predictionsDf != null) {
      val predCounts = predictionsDf.groupBy("predicted_label").count().collect().map(r => r.getString(0) -> r.getLong(1)).toMap
      val benign = predCounts.getOrElse("BENIGN", 0L)
      val attack = predCounts.getOrElse("ATTACK", 0L)
      totalBenign.addAndGet(benign)
      totalAttack.addAndGet(attack)
    }

    // 2. Persist Predictions
    val predBatchParquetPath = s"$outputPath/predictions/parquet/batch_$batchId"
    val predBatchCsvPath = s"$outputPath/predictions/csv/batch_$batchId"

    if (predictionsDf != null && validCount > 0) {
      predictionsDf.write.mode("overwrite").parquet(predBatchParquetPath)
      predictionsDf.coalesce(1).write.mode("overwrite").option("header", "true").csv(predBatchCsvPath)
    }

    // 3. Persist Quarantine (Invalid / Malformed records)
    if (invalidCount > 0) {
      val quarantinePath = s"$outputPath/quarantine/parquet/batch_$batchId"
      invalidDf
        .withColumn("inference_status", when(col("is_oversized"), "OVERSIZED").when(col("has_invalid_numeric"), "INVALID_NUMERIC").otherwise("MALFORMED_JSON"))
        .withColumn("predicted_label", lit("REJECTED"))
        .withColumn("processing_time", current_timestamp().cast(StringType))
        .write.mode("overwrite").parquet(quarantinePath)
    }

    val t1 = System.currentTimeMillis()
    val durationMs = t1 - t0

    println(f"[Micro-Batch $batchId] Processed $batchCount records in $durationMs%d ms (Valid: $validCount, Rejected: $invalidCount, CumValid: ${totalValid.get()}%d)")
  }

  // ── 5. Main Application Entry Point ─────────────────────────

  def parseArgs(args: Array[String]): MLStreamConfig = {
    var config = MLStreamConfig()
    var i = 0
    while (i < args.length) {
      args(i) match {
        case "--topic" if i + 1 < args.length =>
          config = config.copy(topic = args(i + 1)); i += 2
        case "--bootstrap-servers" if i + 1 < args.length =>
          config = config.copy(bootstrapServers = args(i + 1)); i += 2
        case "--checkpoint" if i + 1 < args.length =>
          config = config.copy(checkpointPath = args(i + 1)); i += 2
        case "--output" if i + 1 < args.length =>
          config = config.copy(outputPath = args(i + 1)); i += 2
        case "--model" if i + 1 < args.length =>
          config = config.copy(modelPath = args(i + 1)); i += 2
        case "--duration-seconds" if i + 1 < args.length =>
          config = config.copy(durationSeconds = Some(args(i + 1).toLong)); i += 2
        case "--max-payload-bytes" if i + 1 < args.length =>
          config = config.copy(maxPayloadBytes = args(i + 1).toLong); i += 2
        case other =>
          log.warn(s"Unrecognized argument: $other"); i += 1
      }
    }
    config
  }

  def main(args: Array[String]): Unit = {
    val config = parseArgs(args)

    println("\n" + "=" * 75)
    println("  SparkShield — Milestone 6: Real-Time ML Streaming Inference")
    println(s"  Kafka Broker      : ${config.bootstrapServers}")
    println(s"  Topic             : ${config.topic}")
    println(s"  Model Path        : ${config.modelPath}")
    println(s"  Checkpoint Dir    : ${config.checkpointPath}")
    println(s"  Output Dir        : ${config.outputPath}")
    println(s"  Duration Limit    : ${config.durationSeconds.map(s => s"${s}s").getOrElse("unlimited")}")
    println("=" * 75 + "\n")

    val spark = SparkSessionFactory.create(
      "SparkShield-ML-Streaming-Inference",
      extras = Map(
        "spark.driver.memory" -> "3g",
        "spark.sql.shuffle.partitions" -> "4",
        "spark.sql.constraintPropagation.enabled" -> "false"
      )
    )

    // Performance counters
    val totalConsumed = new AtomicLong(0L)
    val totalValid = new AtomicLong(0L)
    val totalRejected = new AtomicLong(0L)
    val totalBenign = new AtomicLong(0L)
    val totalAttack = new AtomicLong(0L)
    val batchCounter = new AtomicLong(0L)

    try {
      // 1. Load Model & Extract Contract
      val (model, featureNames) = loadModelAndContract(config.modelPath)
      val eventSchema = buildEventSchema(featureNames)
      val modelVersion = new File(config.modelPath).getParentFile.getName

      // 2. Ingest from Kafka
      val rawStream = spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", config.bootstrapServers)
        .option("subscribe", config.topic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()

      // 3. Parse and Validate
      val validatedStream = parseAndValidateStream(
        rawStream       = rawStream,
        schema          = eventSchema,
        featureNames    = featureNames,
        maxPayloadBytes = config.maxPayloadBytes
      )

      // 4. ForeachBatch Streaming Query
      val startTime = System.currentTimeMillis()

      val query: StreamingQuery = validatedStream.writeStream
        .outputMode(OutputMode.Append())
        .trigger(Trigger.ProcessingTime("1 second"))
        .option("checkpointLocation", config.checkpointPath)
        .foreachBatch { (batchDf: DataFrame, batchId: Long) =>
          batchCounter.incrementAndGet()
          processMicroBatch(
            batchDf         = batchDf,
            batchId         = batchId,
            model           = model,
            modelVersion    = modelVersion,
            outputPath      = config.outputPath,
            totalConsumed   = totalConsumed,
            totalValid      = totalValid,
            totalRejected   = totalRejected,
            totalBenign     = totalBenign,
            totalAttack     = totalAttack
          )
        }
        .start()

      println("Streaming queries started successfully. Awaiting flow events from Kafka...\n")

      // Handle bounded duration or await termination
      config.durationSeconds match {
        case Some(sec) =>
          println(s"Running streaming query for $sec seconds...")
          query.awaitTermination(sec * 1000L)
          query.stop()
        case None =>
          println("Running streaming query indefinitely. Press Ctrl+C to terminate.")
          query.awaitTermination()
      }

      val totalTimeSec = (System.currentTimeMillis() - startTime) / 1000.0
      val throughput = if (totalTimeSec > 0) totalConsumed.get() / totalTimeSec else 0.0

      // 5. Save Operational Metrics Report
      val metricsDir = new File(s"${config.outputPath}/metrics")
      if (!metricsDir.exists()) metricsDir.mkdirs()

      val metricsJson =
        s"""{
           |  "run_timestamp": "${Instant.now()}",
           |  "model_version": "$modelVersion",
           |  "kafka_topic": "${config.topic}",
           |  "total_consumed": ${totalConsumed.get()},
           |  "total_valid_inferences": ${totalValid.get()},
           |  "total_rejected": ${totalRejected.get()},
           |  "predicted_benign": ${totalBenign.get()},
           |  "predicted_attack": ${totalAttack.get()},
           |  "total_micro_batches": ${batchCounter.get()},
           |  "total_duration_seconds": $totalTimeSec,
           |  "events_per_second": $throughput
           |}""".stripMargin

      val pw = new PrintWriter(new File(s"${config.outputPath}/metrics/streaming-metrics.json"))
      try pw.write(metricsJson) finally pw.close()

      println("\n" + "=" * 75)
      println("  SparkShield — Streaming ML Inference Execution Summary")
      println("=" * 75)
      println(f"  Total Events Consumed : ${totalConsumed.get()}%d")
      println(f"  Successful Inferences : ${totalValid.get()}%d")
      println(f"  Rejected / Malformed  : ${totalRejected.get()}%d")
      println(f"  Predicted BENIGN      : ${totalBenign.get()}%d")
      println(f"  Predicted ATTACK      : ${totalAttack.get()}%d")
      println(f"  Execution Duration    : $totalTimeSec%.2f s")
      println(f"  Throughput            : $throughput%.2f events/sec")
      println(s"  Metrics Saved To      : ${config.outputPath}/metrics/streaming-metrics.json")
      println(s"  Predictions Output    : ${config.outputPath}/predictions/")
      println("=" * 75 + "\n")

    } finally {
      spark.stop()
    }
  }
}
