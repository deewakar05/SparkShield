package sparkshield

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{StandardScaler, VectorAssembler}
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite

import java.io.File
import java.nio.file.Files
import java.util.concurrent.atomic.AtomicLong

/**
 * Unit tests for NetworkFlowMLStreaming (Milestone 6).
 *
 * Verifies model contract extraction, schema building, stream validation,
 * probability bounds, and micro-batch inference using synthetic fixtures.
 */
class NetworkFlowMLStreamingSpec extends AnyFunSuite with SharedSparkSession {

  private val testFeatures = Array("destination_port", "flow_duration", "flow_bytes_s")

  /** Helper to train and save a small synthetic pipeline model for unit tests. */
  private def createTestModel(dir: File): String = {
    val schema = StructType(Seq(
      StructField("destination_port", DoubleType, nullable = true),
      StructField("flow_duration", DoubleType, nullable = true),
      StructField("flow_bytes_s", DoubleType, nullable = true),
      StructField("label", DoubleType, nullable = false)
    ))

    val rows = Seq(
      Row(80.0, 100.0, 500.0, 0.0),
      Row(443.0, 150.0, 800.0, 0.0),
      Row(50000.0, 5000.0, 50000.0, 1.0),
      Row(55000.0, 6000.0, 60000.0, 1.0)
    )

    val trainDf = spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)

    val assembler = new VectorAssembler()
      .setInputCols(testFeatures)
      .setOutputCol("raw_features")

    val scaler = new StandardScaler()
      .setInputCol("raw_features")
      .setOutputCol("features")
      .setWithStd(true)
      .setWithMean(false)

    val lr = new LogisticRegression()
      .setFeaturesCol("features")
      .setLabelCol("label")
      .setMaxIter(5)
      .setRegParam(0.01)

    val pipeline = new Pipeline().setStages(Array(assembler, scaler, lr))
    val model = pipeline.fit(trainDf)
    val modelPath = s"${dir.getAbsolutePath}/test_model"
    model.write.overwrite().save(modelPath)
    modelPath
  }

  // ── 1. Model Loading & Feature Contract ─────────────────────

  test("loadModelAndContract loads model and extracts ordered feature contract") {
    val tempDir = Files.createTempDirectory("ml-streaming-contract-test-").toFile
    try {
      val modelPath = createTestModel(tempDir)
      val (model, featureNames) = NetworkFlowMLStreaming.loadModelAndContract(modelPath)

      assert(model != null)
      assert(featureNames === testFeatures)
    } finally {
      deleteRecursively(tempDir)
    }
  }

  test("loadModelAndContract throws IllegalArgumentException for missing model path") {
    intercept[IllegalArgumentException] {
      NetworkFlowMLStreaming.loadModelAndContract("/path/does/not/exist/model")
    }
  }

  test("buildEventSchema constructs StructType containing metadata and all required features") {
    val schema = NetworkFlowMLStreaming.buildEventSchema(testFeatures)
    val fieldNames = schema.fieldNames

    assert(fieldNames.contains("flow_id"))
    assert(fieldNames.contains("timestamp"))
    assert(fieldNames.contains("label_binary"))
    assert(fieldNames.contains("label_original"))
    assert(fieldNames.contains("has_invalid_numeric"))
    testFeatures.foreach { f =>
      assert(fieldNames.contains(f))
      assert(schema(f).dataType === DoubleType)
    }
  }

  // ── 2. Stream Ingestion & Parsing Tests ──────────────────────

  private def makeRawKafkaDf(jsonRecords: Seq[String]) = {
    val schema = StructType(Seq(
      StructField("key", StringType, nullable = true),
      StructField("value", StringType, nullable = true)
    ))
    val rows = jsonRecords.zipWithIndex.map { case (json, idx) => Row(s"key_$idx", json) }
    spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)
  }

  test("parseAndValidateStream validates normal JSON event and flags valid records") {
    val schema = NetworkFlowMLStreaming.buildEventSchema(testFeatures)
    val validJson =
      """{
        |  "flow_id": "flow_1",
        |  "timestamp": "2026-10-24 12:00:00",
        |  "destination_port": 80.0,
        |  "flow_duration": 100.0,
        |  "flow_bytes_s": 500.0,
        |  "label_binary": "BENIGN",
        |  "label_original": "BENIGN",
        |  "has_invalid_numeric": false
        |}""".stripMargin

    val rawDf = makeRawKafkaDf(Seq(validJson))
    val validated = NetworkFlowMLStreaming.parseAndValidateStream(rawDf, schema, testFeatures)
    val row = validated.collect().head

    assert(row.getAs[Boolean]("is_valid") === true)
    assert(row.getAs[String]("flow_id") === "flow_1")
    assert(row.getAs[Double]("destination_port") === 80.0)
    assert(row.getAs[Double]("flow_duration") === 100.0)
    assert(row.getAs[Double]("flow_bytes_s") === 500.0)
  }

  test("parseAndValidateStream rejects malformed JSON and missing flow_id") {
    val schema = NetworkFlowMLStreaming.buildEventSchema(testFeatures)
    val malformedJson = "{ not a valid json }"
    val missingFlowIdJson =
      """{
        |  "destination_port": 80.0,
        |  "flow_duration": 100.0,
        |  "flow_bytes_s": 500.0
        |}""".stripMargin

    val rawDf = makeRawKafkaDf(Seq(malformedJson, missingFlowIdJson))
    val validated = NetworkFlowMLStreaming.parseAndValidateStream(rawDf, schema, testFeatures)
    val rows = validated.collect()

    assert(rows.forall(r => !r.getAs[Boolean]("is_valid")))
  }

  test("parseAndValidateStream rejects non-finite features (NaN, Infinity, null)") {
    val schema = NetworkFlowMLStreaming.buildEventSchema(testFeatures)
    val nanJson =
      """{
        |  "flow_id": "flow_nan",
        |  "destination_port": 80.0,
        |  "flow_duration": "NaN",
        |  "flow_bytes_s": 500.0
        |}""".stripMargin
    val infJson =
      """{
        |  "flow_id": "flow_inf",
        |  "destination_port": 80.0,
        |  "flow_duration": 100.0,
        |  "flow_bytes_s": "Infinity"
        |}""".stripMargin

    val rawDf = makeRawKafkaDf(Seq(nanJson, infJson))
    val validated = NetworkFlowMLStreaming.parseAndValidateStream(rawDf, schema, testFeatures)
    val rows = validated.collect()

    assert(rows.forall(r => !r.getAs[Boolean]("is_valid")))
    assert(rows.forall(r => r.getAs[Boolean]("has_invalid_numeric")))
  }

  test("parseAndValidateStream rejects oversized messages exceeding maxPayloadBytes (SEC-05)") {
    val schema = NetworkFlowMLStreaming.buildEventSchema(testFeatures)
    val hugePayload = "x" * 200
    val normalPayload = """{"flow_id":"f1","destination_port":80.0,"flow_duration":10.0,"flow_bytes_s":1.0}"""

    val rawDf = makeRawKafkaDf(Seq(hugePayload, normalPayload))
    // Set max payload to 100 bytes
    val validated = NetworkFlowMLStreaming.parseAndValidateStream(rawDf, schema, testFeatures, maxPayloadBytes = 100L)
    val rows = validated.collect()

    val oversizedRow = rows.find(r => r.getAs[Long]("payload_bytes") > 100L).get
    assert(oversizedRow.getAs[Boolean]("is_oversized") === true)
    assert(oversizedRow.getAs[Boolean]("is_valid") === false)
  }

  // ── 3. Model Inference & Probability Calculation ────────────

  test("applyInference generates correct predictions, valid probabilities in [0.0, 1.0], and sums to ~1.0") {
    val tempDir = Files.createTempDirectory("ml-streaming-inference-test-").toFile
    try {
      val modelPath = createTestModel(tempDir)
      val (model, _) = NetworkFlowMLStreaming.loadModelAndContract(modelPath)

      val rows = Seq(
        // Benign-like traffic
        Row("key_1", "{}", 10L, false, true, false, "flow_b1", "2026-10-24 12:00:00", "BENIGN", "BENIGN", 80.0, 100.0, 500.0),
        // Attack-like traffic
        Row("key_2", "{}", 10L, false, true, false, "flow_a1", "2026-10-24 12:00:01", "ATTACK", "DDoS", 50000.0, 5000.0, 50000.0)
      )

      val schema = StructType(Seq(
        StructField("kafka_key", StringType),
        StructField("raw_payload", StringType),
        StructField("payload_bytes", LongType),
        StructField("is_oversized", BooleanType),
        StructField("is_valid", BooleanType),
        StructField("has_invalid_numeric", BooleanType),
        StructField("flow_id", StringType),
        StructField("event_timestamp", StringType),
        StructField("ground_truth_label", StringType),
        StructField("ground_truth_original", StringType),
        StructField("destination_port", DoubleType),
        StructField("flow_duration", DoubleType),
        StructField("flow_bytes_s", DoubleType)
      ))

      val validDf = spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)
      val scored = NetworkFlowMLStreaming.applyInference(validDf, model, "test-v1")

      assert(scored.columns.contains("predicted_label"))
      assert(scored.columns.contains("probability_benign"))
      assert(scored.columns.contains("probability_attack"))
      assert(scored.columns.contains("inference_status"))
      assert(scored.columns.contains("is_correct"))

      val scoredRows = scored.collect()
      assert(scoredRows.length === 2)

      scoredRows.foreach { r =>
        val pBenign = r.getAs[Double]("probability_benign")
        val pAttack = r.getAs[Double]("probability_attack")
        val label = r.getAs[String]("predicted_label")

        assert(pBenign >= 0.0 && pBenign <= 1.0)
        assert(pAttack >= 0.0 && pAttack <= 1.0)
        assert(math.abs((pBenign + pAttack) - 1.0) < 1e-4)

        if (label == "BENIGN") assert(pBenign >= 0.5)
        else if (label == "ATTACK") assert(pAttack >= 0.5)
      }

      val benignFlow = scoredRows.find(_.getAs[String]("flow_id") == "flow_b1").get
      val attackFlow = scoredRows.find(_.getAs[String]("flow_id") == "flow_a1").get

      assert(benignFlow.getAs[String]("predicted_label") === "BENIGN")
      assert(attackFlow.getAs[String]("predicted_label") === "ATTACK")
      assert(benignFlow.getAs[Boolean]("is_correct") === true)
      assert(attackFlow.getAs[Boolean]("is_correct") === true)
    } finally {
      deleteRecursively(tempDir)
    }
  }

  // ── 4. Micro-Batch Idempotency & Persistence ────────────────

  test("processMicroBatch writes valid micro-batches idempotently and handles retries without duplicate data") {
    val tempDir = Files.createTempDirectory("ml-streaming-batch-test-").toFile
    try {
      val modelPath = createTestModel(tempDir)
      val (model, _) = NetworkFlowMLStreaming.loadModelAndContract(modelPath)

      val rows = Seq(
        Row("key_1", "{}", 10L, false, true, false, "flow_1", "2026-10-24 12:00:00", "BENIGN", "BENIGN", 80.0, 100.0, 500.0),
        Row("key_2", "{}", 10L, false, false, true, "flow_bad", "2026-10-24 12:00:01", "ATTACK", "DDoS", 50000.0, 5000.0, 50000.0)
      )

      val schema = StructType(Seq(
        StructField("kafka_key", StringType),
        StructField("raw_payload", StringType),
        StructField("payload_bytes", LongType),
        StructField("is_oversized", BooleanType),
        StructField("is_valid", BooleanType),
        StructField("has_invalid_numeric", BooleanType),
        StructField("flow_id", StringType),
        StructField("event_timestamp", StringType),
        StructField("ground_truth_label", StringType),
        StructField("ground_truth_original", StringType),
        StructField("destination_port", DoubleType),
        StructField("flow_duration", DoubleType),
        StructField("flow_bytes_s", DoubleType)
      ))

      val batchDf = spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)

      val totalConsumed = new AtomicLong(0L)
      val totalValid = new AtomicLong(0L)
      val totalRejected = new AtomicLong(0L)
      val totalBenign = new AtomicLong(0L)
      val totalAttack = new AtomicLong(0L)

      val outputPath = s"${tempDir.getAbsolutePath}/output"

      // Execute batch 1
      NetworkFlowMLStreaming.processMicroBatch(
        batchDf, 1L, model, "test-v1", outputPath,
        totalConsumed, totalValid, totalRejected, totalBenign, totalAttack
      )

      val predParquet1 = spark.read.parquet(s"$outputPath/predictions/parquet/batch_1")
      assert(predParquet1.count() === 1L)

      // Retry batch 1 (simulating crash retry with identical batchId)
      NetworkFlowMLStreaming.processMicroBatch(
        batchDf, 1L, model, "test-v1", outputPath,
        totalConsumed, totalValid, totalRejected, totalBenign, totalAttack
      )

      val predParquetRetry = spark.read.parquet(s"$outputPath/predictions/parquet/batch_1")
      // Overwrite mode guarantees exactly 1 record, not duplicate 2
      assert(predParquetRetry.count() === 1L)
    } finally {
      deleteRecursively(tempDir)
    }
  }

  test("processMicroBatch safely handles empty micro-batches without error") {
    val tempDir = Files.createTempDirectory("ml-streaming-empty-test-").toFile
    try {
      val modelPath = createTestModel(tempDir)
      val (model, _) = NetworkFlowMLStreaming.loadModelAndContract(modelPath)

      val schema = StructType(Seq(
        StructField("kafka_key", StringType),
        StructField("raw_payload", StringType),
        StructField("payload_bytes", LongType),
        StructField("is_oversized", BooleanType),
        StructField("is_valid", BooleanType),
        StructField("has_invalid_numeric", BooleanType),
        StructField("flow_id", StringType),
        StructField("event_timestamp", StringType),
        StructField("ground_truth_label", StringType),
        StructField("ground_truth_original", StringType),
        StructField("destination_port", DoubleType),
        StructField("flow_duration", DoubleType),
        StructField("flow_bytes_s", DoubleType)
      ))

      val emptyDf = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)

      val totalConsumed = new AtomicLong(0L)
      val totalValid = new AtomicLong(0L)
      val totalRejected = new AtomicLong(0L)
      val totalBenign = new AtomicLong(0L)
      val totalAttack = new AtomicLong(0L)

      NetworkFlowMLStreaming.processMicroBatch(
        emptyDf, 42L, model, "test-v1", s"${tempDir.getAbsolutePath}/output",
        totalConsumed, totalValid, totalRejected, totalBenign, totalAttack
      )

      assert(totalConsumed.get() === 0L)
      assert(totalValid.get() === 0L)
    } finally {
      deleteRecursively(tempDir)
    }
  }

  test("resolveDefaultModelPath resolves valid model path and parseArgs respects --model flag") {
    val defaultPath = NetworkFlowMLStreaming.resolveDefaultModelPath()
    assert(defaultPath.nonEmpty)

    val args = Array(
      "--topic", "test-topic",
      "--model", "custom/test/model/path",
      "--checkpoint", "custom/checkpoint",
      "--output", "custom/output",
      "--duration-seconds", "45",
      "--max-payload-bytes", "32768"
    )
    val config = NetworkFlowMLStreaming.parseArgs(args)
    assert(config.topic === "test-topic")
    assert(config.modelPath === "custom/test/model/path")
    assert(config.checkpointPath === "custom/checkpoint")
    assert(config.outputPath === "custom/output")
    assert(config.durationSeconds === Some(45L))
    assert(config.maxPayloadBytes === 32768L)
  }
}
