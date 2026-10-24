package sparkshield

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.scalatest.funsuite.AnyFunSuite

import java.io.File
import java.nio.file.Files

/**
 * Unit tests for NetworkFlowStreaming and NetworkFlowProducer (Milestone 4).
 *
 * Runs completely independent of a live Kafka broker using synthetic
 * Kafka-format DataFrames (key, value pairs).
 */
class NetworkFlowStreamingSpec extends AnyFunSuite with SharedSparkSession {

  // ── Fixture Helpers ─────────────────────────────────────────

  private def makeRawKafkaDf(records: Seq[(String, String)]) = {
    val schema = StructType(Seq(
      StructField("key", StringType, nullable = true),
      StructField("value", StringType, nullable = true)
    ))
    val rows = records.map { case (k, v) => Row(k, v) }
    spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)
  }

  private val validBenignJson =
    """{
      |  "flow_id": "flow_1",
      |  "timestamp": "2026-10-24 12:00:10",
      |  "destination_port": 80.0,
      |  "flow_duration": 150.0,
      |  "total_fwd_packets": 3.0,
      |  "total_backward_packets": 2.0,
      |  "flow_bytes_s": 2500.0,
      |  "flow_packets_s": 50.0,
      |  "has_invalid_numeric": false,
      |  "label_original": "BENIGN",
      |  "label_binary": "BENIGN"
      |}""".stripMargin

  private val validAttackJson =
    """{
      |  "flow_id": "flow_2",
      |  "timestamp": "2026-10-24 12:00:25",
      |  "destination_port": 443.0,
      |  "flow_duration": 300.0,
      |  "total_fwd_packets": 6.0,
      |  "total_backward_packets": 5.0,
      |  "flow_bytes_s": 6000.0,
      |  "flow_packets_s": 120.0,
      |  "has_invalid_numeric": false,
      |  "label_original": "DDoS",
      |  "label_binary": "ATTACK"
      |}""".stripMargin

  private val unknownAttackJson =
    """{
      |  "flow_id": "flow_3",
      |  "timestamp": "2026-10-24 12:00:40",
      |  "destination_port": 22.0,
      |  "flow_duration": 80.0,
      |  "total_fwd_packets": 1.0,
      |  "total_backward_packets": 1.0,
      |  "flow_bytes_s": 1200.0,
      |  "flow_packets_s": 20.0,
      |  "has_invalid_numeric": false,
      |  "label_original": "ZeroDayBotnet",
      |  "label_binary": "ATTACK"
      |}""".stripMargin

  private val invalidNumericJson =
    """{
      |  "flow_id": "flow_4",
      |  "timestamp": "2026-10-24 12:00:50",
      |  "destination_port": 8080.0,
      |  "flow_duration": 0.0,
      |  "total_fwd_packets": 1.0,
      |  "total_backward_packets": 0.0,
      |  "flow_bytes_s": null,
      |  "flow_packets_s": null,
      |  "has_invalid_numeric": true,
      |  "label_original": "PortScan",
      |  "label_binary": "ATTACK"
      |}""".stripMargin

  private val malformedJson = "{ corrupted_json: [ unclosed "

  private val missingFieldsJson = """{ "flow_id": "flow_missing" }"""

  // ── 1. JSON Parsing & Schema Validation ─────────────────────

  test("parseAndValidateStream parses valid flow events accurately") {
    val rawDf = makeRawKafkaDf(Seq(
      ("k1", validBenignJson),
      ("k2", validAttackJson)
    ))
    val parsed = NetworkFlowStreaming.parseAndValidateStream(rawDf)

    assert(parsed.count() === 2)
    val validRows = parsed.filter(col("is_valid") === true).collect()
    assert(validRows.length === 2)

    val row1 = validRows.find(_.getAs[String]("flow_id") == "flow_1").get
    assert(row1.getAs[Double]("destination_port") === 80.0)
    assert(row1.getAs[String]("label_original") === "BENIGN")
    assert(row1.getAs[String]("label_binary") === "BENIGN")
    assert(row1.getAs[Boolean]("has_invalid_numeric") === false)
    assert(row1.getAs[java.sql.Timestamp]("event_time") != null)
  }

  test("parseAndValidateStream identifies and flags malformed JSON as invalid") {
    val rawDf = makeRawKafkaDf(Seq(
      ("k1", validBenignJson),
      ("k2", malformedJson)
    ))
    val parsed = NetworkFlowStreaming.parseAndValidateStream(rawDf)

    assert(parsed.count() === 2)
    val validCount = parsed.filter(col("is_valid") === true).count()
    val invalidCount = parsed.filter(col("is_valid") === false).count()

    assert(validCount === 1)
    assert(invalidCount === 1)

    val invalidRow = parsed.filter(col("is_valid") === false).head()
    assert(invalidRow.getAs[String]("raw_payload") === malformedJson)
  }

  test("parseAndValidateStream flags missing required fields as invalid") {
    val rawDf = makeRawKafkaDf(Seq(
      ("k_missing", missingFieldsJson)
    ))
    val parsed = NetworkFlowStreaming.parseAndValidateStream(rawDf)

    assert(parsed.count() === 1)
    assert(parsed.filter(col("is_valid") === true).count() === 0)
    assert(parsed.filter(col("is_valid") === false).count() === 1)
  }

  test("parseAndValidateStream preserves unknown attack labels without defaulting to BENIGN") {
    val rawDf = makeRawKafkaDf(Seq(
      ("k_unknown", unknownAttackJson)
    ))
    val parsed = NetworkFlowStreaming.parseAndValidateStream(rawDf)
    val valid = parsed.filter(col("is_valid") === true).head()

    assert(valid.getAs[String]("label_original") === "ZeroDayBotnet")
    assert(valid.getAs[String]("label_binary") === "ATTACK")
  }

  test("parseAndValidateStream correctly preserves has_invalid_numeric flag") {
    val rawDf = makeRawKafkaDf(Seq(
      ("k_inv", invalidNumericJson)
    ))
    val parsed = NetworkFlowStreaming.parseAndValidateStream(rawDf)
    val row = parsed.filter(col("is_valid") === true).head()

    assert(row.getAs[Boolean]("has_invalid_numeric") === true)
    assert(row.getAs[String]("label_original") === "PortScan")
  }

  // ── 2. Aggregations (Binary, Label, Windowed) ───────────────

  test("computeBinarySummary calculates correct benign and attack counts") {
    val rawDf = makeRawKafkaDf(Seq(
      ("k1", validBenignJson),
      ("k2", validAttackJson),
      ("k3", unknownAttackJson)
    ))
    val valid = NetworkFlowStreaming.parseAndValidateStream(rawDf).filter(col("is_valid") === true)
    val summary = NetworkFlowStreaming.computeBinarySummary(valid).collect()

    val counts = summary.map(r => r.getAs[String]("label_binary") -> r.getAs[Long]("flow_count")).toMap
    assert(counts("BENIGN") === 1L)
    assert(counts("ATTACK") === 2L)
  }

  test("computeAttackLabelSummary groups attacks correctly by original label") {
    val rawDf = makeRawKafkaDf(Seq(
      ("k1", validBenignJson),
      ("k2", validAttackJson),
      ("k3", unknownAttackJson),
      ("k4", invalidNumericJson)
    ))
    val valid = NetworkFlowStreaming.parseAndValidateStream(rawDf).filter(col("is_valid") === true)
    val attackSummary = NetworkFlowStreaming.computeAttackLabelSummary(valid).collect()

    val counts = attackSummary.map(r => r.getAs[String]("label_original") -> r.getAs[Long]("attack_count")).toMap
    assert(!counts.contains("BENIGN"))
    assert(counts("DDoS") === 1L)
    assert(counts("ZeroDayBotnet") === 1L)
    assert(counts("PortScan") === 1L)
  }

  test("computeWindowedTraffic groups events into deterministic time windows") {
    val rawDf = makeRawKafkaDf(Seq(
      ("k1", validBenignJson), // 12:00:10
      ("k2", validAttackJson), // 12:00:25
      ("k3", unknownAttackJson) // 12:00:40
    ))
    val valid = NetworkFlowStreaming.parseAndValidateStream(rawDf).filter(col("is_valid") === true)

    // Window: 1 minute, Slide: 1 minute
    val windowed = NetworkFlowStreaming.computeWindowedTraffic(valid, "10 seconds", "1 minute", "1 minute")
    val rows = windowed.collect()

    assert(rows.nonEmpty)
    val totalFlowsInWindows = rows.map(_.getAs[Long]("flow_count")).sum
    assert(totalFlowsInWindows === 3L)
  }

  test("computeBinarySummary on empty stream returns empty aggregation without error") {
    val rawDf = makeRawKafkaDf(Seq.empty)
    val valid = NetworkFlowStreaming.parseAndValidateStream(rawDf).filter(col("is_valid") === true)
    val summary = NetworkFlowStreaming.computeBinarySummary(valid)

    assert(summary.count() === 0L)
  }

  // ── 3. Micro-batch & Persistence Tests ──────────────────────

  test("processMicroBatch safely handles empty micro-batches without creating corrupt files") {
    val tempDir = Files.createTempDirectory("streaming_test_empty_").toFile
    try {
      val rawDf = makeRawKafkaDf(Seq.empty)
      NetworkFlowStreaming.processMicroBatch(rawDf, batchId = 0L, tempDir.getPath)

      // Nothing should be written for empty batch
      assert(!new File(tempDir, "parquet/batch_0").exists())
      assert(!new File(tempDir, "csv/batch_0").exists())
    } finally {
      deleteRecursively(tempDir)
    }
  }

  test("processMicroBatch writes valid micro-batches to Parquet and CSV") {
    val tempDir = Files.createTempDirectory("streaming_test_batch_").toFile
    try {
      val rawDf = makeRawKafkaDf(Seq(
        ("k1", validBenignJson),
        ("k2", validAttackJson)
      ))
      val valid = NetworkFlowStreaming.parseAndValidateStream(rawDf).filter(col("is_valid") === true)

      NetworkFlowStreaming.processMicroBatch(valid, batchId = 1L, tempDir.getPath)

      // Check Parquet batch output
      val parquetDir = new File(tempDir, "parquet/batch_1")
      assert(parquetDir.exists())
      val readParquet = spark.read.parquet(parquetDir.getPath)
      assert(readParquet.count() === 2L)

      // Check CSV batch output
      val csvDir = new File(tempDir, "csv/batch_1")
      assert(csvDir.exists())
      val readCsv = spark.read.option("header", "true").option("multiLine", "true").csv(csvDir.getPath)
      assert(readCsv.count() === 2L)
    } finally {
      deleteRecursively(tempDir)
    }
  }

  // ── 4. Producer Serialization Tests ─────────────────────────

  test("NetworkFlowProducer.prepareJsonDataFrame generates valid JSON matching FlowEventSchema") {
    val schema = StructType(Seq(
      StructField("destination_port", DoubleType, nullable = true),
      StructField("flow_duration", DoubleType, nullable = true),
      StructField("label_original", StringType, nullable = true),
      StructField("label_binary", StringType, nullable = false),
      StructField("has_invalid_numeric", BooleanType, nullable = true)
    ))
    val inputDf = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row(80.0, 100.0, "BENIGN", "BENIGN", false))),
      schema
    )

    val jsonDf = NetworkFlowProducer.prepareJsonDataFrame(inputDf)
    assert(jsonDf.columns.toSeq === Seq("key", "value"))

    val rows = jsonDf.collect()
    assert(rows.length === 1)
    val key = rows.head.getAs[String]("key")
    val value = rows.head.getAs[String]("value")

    assert(key.startsWith("flow_"))
    assert(value.contains("\"destination_port\":80.0"))
    assert(value.contains("\"label_original\":\"BENIGN\""))
    assert(value.contains("\"timestamp\""))

    // Verify it parses back through NetworkFlowStreaming
    val parsed = NetworkFlowStreaming.parseAndValidateStream(jsonDf)
    assert(parsed.filter(col("is_valid") === true).count() === 1L)
  }

  // ── 5. Hardening & Regression Tests ─────────────────────────

  test("non-finite numeric values (NaN and Infinity) do not corrupt averages to Infinity or NaN") {
    val infJson =
      """{
        |  "flow_id": "flow_inf",
        |  "timestamp": "2026-10-24 12:00:15",
        |  "destination_port": 80.0,
        |  "flow_duration": 100.0,
        |  "total_fwd_packets": 2.0,
        |  "total_backward_packets": 2.0,
        |  "flow_bytes_s": "Infinity",
        |  "flow_packets_s": 40.0,
        |  "has_invalid_numeric": true,
        |  "label_original": "DDoS",
        |  "label_binary": "ATTACK"
        |}""".stripMargin

    val nanJson =
      """{
        |  "flow_id": "flow_nan",
        |  "timestamp": "2026-10-24 12:00:20",
        |  "destination_port": 443.0,
        |  "flow_duration": 200.0,
        |  "total_fwd_packets": 5.0,
        |  "total_backward_packets": 5.0,
        |  "flow_bytes_s": "NaN",
        |  "flow_packets_s": 50.0,
        |  "has_invalid_numeric": true,
        |  "label_original": "DDoS",
        |  "label_binary": "ATTACK"
        |}""".stripMargin

    val finiteJson1 =
      """{
        |  "flow_id": "flow_finite1",
        |  "timestamp": "2026-10-24 12:00:25",
        |  "destination_port": 80.0,
        |  "flow_duration": 100.0,
        |  "total_fwd_packets": 1.0,
        |  "total_backward_packets": 1.0,
        |  "flow_bytes_s": 1000.0,
        |  "flow_packets_s": 20.0,
        |  "has_invalid_numeric": false,
        |  "label_original": "DDoS",
        |  "label_binary": "ATTACK"
        |}""".stripMargin

    val finiteJson2 =
      """{
        |  "flow_id": "flow_finite2",
        |  "timestamp": "2026-10-24 12:00:30",
        |  "destination_port": 80.0,
        |  "flow_duration": 300.0,
        |  "total_fwd_packets": 2.0,
        |  "total_backward_packets": 2.0,
        |  "flow_bytes_s": 3000.0,
        |  "flow_packets_s": 40.0,
        |  "has_invalid_numeric": false,
        |  "label_original": "DDoS",
        |  "label_binary": "ATTACK"
        |}""".stripMargin

    val rawDf = makeRawKafkaDf(Seq(
      ("k_inf", infJson),
      ("k_nan", nanJson),
      ("k_f1", finiteJson1),
      ("k_f2", finiteJson2)
    ))
    val valid = NetworkFlowStreaming.parseAndValidateStream(rawDf).filter(col("is_valid") === true)

    val summary = NetworkFlowStreaming.computeBinarySummary(valid).collect()
    assert(summary.length === 1)
    val row = summary.head

    assert(row.getAs[Long]("flow_count") === 4L)
    assert(row.getAs[Long]("invalid_numeric_count") === 2L)

    // Finite average of (1000.0 + 3000.0) / 2 = 2000.0. MUST NOT be Infinity or NaN!
    val avgBytes = row.getAs[Double]("avg_bytes_s")
    assert(!avgBytes.isNaN)
    assert(!avgBytes.isInfinite)
    assert(avgBytes === 2000.0)

    val avgDur = row.getAs[Double]("avg_duration")
    assert(!avgDur.isNaN)
    assert(!avgDur.isInfinite)
    assert(avgDur === (100.0 + 200.0 + 100.0 + 300.0) / 4.0)
  }

  test("computeQualitySummary accurately audits valid, malformed, and invalid numeric events") {
    val rawDf = makeRawKafkaDf(Seq(
      ("k1", validBenignJson),
      ("k2", validAttackJson),
      ("k3", invalidNumericJson), // valid JSON but invalid numeric
      ("k4", malformedJson),      // malformed JSON
      ("k5", missingFieldsJson)   // missing required fields
    ))
    val parsed = NetworkFlowStreaming.parseAndValidateStream(rawDf)
    val qualityRow = NetworkFlowStreaming.computeQualitySummary(parsed).head()

    assert(qualityRow.getAs[Long]("total_events") === 5L)
    assert(qualityRow.getAs[Long]("valid_count") === 3L)
    assert(qualityRow.getAs[Long]("malformed_count") === 2L)
    assert(qualityRow.getAs[Long]("invalid_numeric_count") === 1L)
    assert(qualityRow.getAs[Long]("benign_count") === 1L)
    assert(qualityRow.getAs[Long]("attack_count") === 2L)
  }

  test("processMicroBatch is idempotent under batch retry re-execution") {
    val tempDir = Files.createTempDirectory("streaming_test_idempotent_").toFile
    try {
      val rawDf = makeRawKafkaDf(Seq(
        ("k1", validBenignJson),
        ("k2", validAttackJson)
      ))
      val valid = NetworkFlowStreaming.parseAndValidateStream(rawDf).filter(col("is_valid") === true)

      // First run of batch 42
      NetworkFlowStreaming.processMicroBatch(valid, batchId = 42L, tempDir.getPath)
      val count1 = spark.read.parquet(new File(tempDir, "parquet/batch_42").getPath).count()
      assert(count1 === 2L)

      // Simulated retry of batch 42 (must overwrite, NOT duplicate records)
      NetworkFlowStreaming.processMicroBatch(valid, batchId = 42L, tempDir.getPath)
      val count2 = spark.read.parquet(new File(tempDir, "parquet/batch_42").getPath).count()
      assert(count2 === 2L)

      val csvCount = spark.read.option("header", "true").option("multiLine", "true")
        .csv(new File(tempDir, "csv/batch_42").getPath).count()
      assert(csvCount === 2L)
    } finally {
      deleteRecursively(tempDir)
    }
  }

  // ── 6. Deterministic Mixed-Label Regression Test ────────────

  test("deterministic mixed-label regression: 2 BENIGN, 3 ATTACK flows") {
    def makeFlow(id: String, labelOrig: String, labelBin: String, dur: Double, ts: String): String =
      s"""{
         |  "flow_id": "$id",
         |  "timestamp": "$ts",
         |  "destination_port": 80.0,
         |  "flow_duration": $dur,
         |  "total_fwd_packets": 2.0,
         |  "total_backward_packets": 2.0,
         |  "flow_bytes_s": ${dur * 10.0},
         |  "flow_packets_s": 20.0,
         |  "has_invalid_numeric": false,
         |  "label_original": "$labelOrig",
         |  "label_binary": "$labelBin"
         |}""".stripMargin

    val syntheticFlows = Seq(
      ("e1", makeFlow("event-001", "BENIGN", "BENIGN", 100.0, "2026-10-24 12:00:00")),
      ("e2", makeFlow("event-002", "DDoS",   "ATTACK", 200.0, "2026-10-24 12:00:10")),
      ("e3", makeFlow("event-003", "BENIGN", "BENIGN", 300.0, "2026-10-24 12:00:20")),
      ("e4", makeFlow("event-004", "PortScan","ATTACK", 400.0, "2026-10-24 12:00:30")),
      ("e5", makeFlow("event-005", "Botnet", "ATTACK", 500.0, "2026-10-24 12:00:40"))
    )

    val rawDf = makeRawKafkaDf(syntheticFlows)
    val parsed = NetworkFlowStreaming.parseAndValidateStream(rawDf)

    // 1. Quality summary validation
    val qualityRow = NetworkFlowStreaming.computeQualitySummary(parsed).head()
    assert(qualityRow.getAs[Long]("total_events") === 5L)
    assert(qualityRow.getAs[Long]("valid_count") === 5L)
    assert(qualityRow.getAs[Long]("malformed_count") === 0L)
    assert(qualityRow.getAs[Long]("invalid_numeric_count") === 0L)
    assert(qualityRow.getAs[Long]("benign_count") === 2L)
    assert(qualityRow.getAs[Long]("attack_count") === 3L)

    // 2. Binary summary validation
    val valid = parsed.filter(col("is_valid") === true)
    val binarySummary = NetworkFlowStreaming.computeBinarySummary(valid).collect()
    val binaryMap = binarySummary.map { r =>
      r.getAs[String]("label_binary") -> (r.getAs[Long]("flow_count"), r.getAs[Double]("avg_duration"))
    }.toMap

    assert(binaryMap("BENIGN")._1 === 2L)
    assert(binaryMap("BENIGN")._2 === 200.0) // (100 + 300) / 2 = 200.0

    assert(binaryMap("ATTACK")._1 === 3L)
    assert(math.abs(binaryMap("ATTACK")._2 - 366.6667) < 0.01) // (200 + 400 + 500) / 3 = 366.6667

    // 3. Attack label summary
    val attackLabels = NetworkFlowStreaming.computeAttackLabelSummary(valid).collect()
    val attackMap = attackLabels.map(r => r.getAs[String]("label_original") -> r.getAs[Long]("attack_count")).toMap
    assert(attackMap("DDoS") === 1L)
    assert(attackMap("PortScan") === 1L)
    assert(attackMap("Botnet") === 1L)
    assert(!attackMap.contains("BENIGN"))
  }

  test("parseAndValidateStream flags null or empty label_binary as malformed") {
    val nullBinaryJson =
      """{
        |  "flow_id": "flow_null_bin",
        |  "timestamp": "2026-10-24 12:00:00",
        |  "destination_port": 80.0,
        |  "flow_duration": 100.0,
        |  "has_invalid_numeric": false,
        |  "label_original": "BENIGN",
        |  "label_binary": null
        |}""".stripMargin

    val rawDf = makeRawKafkaDf(Seq(("k_null", nullBinaryJson)))
    val parsed = NetworkFlowStreaming.parseAndValidateStream(rawDf)
    val row = parsed.head()

    assert(row.getAs[Boolean]("is_valid") === false)
    assert(row.getAs[Boolean]("is_malformed") === true)
  }

  // ── 7. SEC-05 Streaming Payload Limit Tests ─────────────────

  test("SEC-05: parseAndValidateStream accepts normal message below payload limit") {
    val rawDf = makeRawKafkaDf(Seq(("k_normal", validBenignJson)))
    val parsed = NetworkFlowStreaming.parseAndValidateStream(rawDf)
    val row = parsed.head()

    assert(row.getAs[Long]("payload_bytes") > 0L)
    assert(row.getAs[Long]("payload_bytes") < 65536L)
    assert(row.getAs[Boolean]("is_oversized") === false)
    assert(row.getAs[Boolean]("is_valid") === true)
    assert(row.getAs[Boolean]("is_malformed") === false)
  }

  test("SEC-05: parseAndValidateStream accepts message exactly at 65,536 bytes limit") {
    val prefix = """{"flow_id":"exact_64k","timestamp":"2026-10-24 12:00:00","destination_port":80.0,"flow_duration":100.0,"label_original":"BENIGN","label_binary":"BENIGN","pad":""""
    val suffix = """"}"""
    val baseBytes = (prefix + suffix).getBytes(java.nio.charset.StandardCharsets.UTF_8).length
    val pad = "x" * (65536 - baseBytes)
    val exact64kJson = prefix + pad + suffix

    assert(exact64kJson.getBytes(java.nio.charset.StandardCharsets.UTF_8).length === 65536)

    val rawDf = makeRawKafkaDf(Seq(("k_exact", exact64kJson)))
    val parsed = NetworkFlowStreaming.parseAndValidateStream(rawDf)
    val row = parsed.head()

    assert(row.getAs[Long]("payload_bytes") === 65536L)
    assert(row.getAs[Boolean]("is_oversized") === false)
    assert(row.getAs[Boolean]("is_valid") === true)
    assert(row.getAs[Boolean]("is_malformed") === false)
  }

  test("SEC-05: parseAndValidateStream rejects oversized message (>65,536 bytes) as is_oversized and is_malformed") {
    val prefix = """{"flow_id":"oversized","timestamp":"2026-10-24 12:00:00","destination_port":80.0,"flow_duration":100.0,"label_original":"BENIGN","label_binary":"BENIGN","pad":""""
    val suffix = """"}"""
    val baseBytes = (prefix + suffix).getBytes(java.nio.charset.StandardCharsets.UTF_8).length
    val pad = "x" * (65537 - baseBytes)
    val oversizedJson = prefix + pad + suffix

    assert(oversizedJson.getBytes(java.nio.charset.StandardCharsets.UTF_8).length === 65537)

    val rawDf = makeRawKafkaDf(Seq(("k_over", oversizedJson)))
    val parsed = NetworkFlowStreaming.parseAndValidateStream(rawDf)
    val row = parsed.head()

    assert(row.getAs[Long]("payload_bytes") === 65537L)
    assert(row.getAs[Boolean]("is_oversized") === true)
    assert(row.getAs[Boolean]("is_valid") === false)
    assert(row.getAs[Boolean]("is_malformed") === true)
  }

  test("SEC-05: parseAndValidateStream measures UTF-8 bytes (not character count) for non-ASCII payloads") {
    // 25,000 Chinese characters '中': character count ~25,150 (< 65,536 chars),
    // but UTF-8 byte count = 25,000 * 3 = 75,000+ bytes (> 65,536 bytes).
    val chineseChars = "\u4e2d" * 25000
    val nonAsciiJson =
      s"""{"flow_id":"non_ascii","timestamp":"2026-10-24 12:00:00","destination_port":80.0,"flow_duration":100.0,"label_original":"BENIGN","label_binary":"BENIGN","chinese_payload":"$chineseChars"}"""

    val charLength = nonAsciiJson.length
    val byteLength = nonAsciiJson.getBytes(java.nio.charset.StandardCharsets.UTF_8).length

    assert(charLength < 65536, s"Character length $charLength should be < 65536")
    assert(byteLength > 65536, s"UTF-8 byte length $byteLength should be > 65536")

    val rawDf = makeRawKafkaDf(Seq(("k_non_ascii", nonAsciiJson)))
    val parsed = NetworkFlowStreaming.parseAndValidateStream(rawDf)
    val row = parsed.head()

    assert(row.getAs[Long]("payload_bytes") === byteLength.toLong)
    assert(row.getAs[Boolean]("is_oversized") === true)
    assert(row.getAs[Boolean]("is_valid") === false)
    assert(row.getAs[Boolean]("is_malformed") === true)
  }

  test("SEC-05: rejected and oversized messages cannot contaminate normal analytics") {
    val prefix = """{"flow_id":"oversized","timestamp":"2026-10-24 12:00:00","destination_port":80.0,"flow_duration":100.0,"flow_bytes_s":1000.0,"flow_packets_s":10.0,"label_original":"DDoS","label_binary":"ATTACK","pad":""""
    val suffix = """"}"""
    val baseBytes = (prefix + suffix).getBytes(java.nio.charset.StandardCharsets.UTF_8).length
    val pad = "x" * (70000 - baseBytes)
    val oversizedAttackJson = prefix + pad + suffix

    val rawDf = makeRawKafkaDf(Seq(
      ("k1", validBenignJson),
      ("k2", validAttackJson),
      ("k3", oversizedAttackJson)
    ))

    val parsed = NetworkFlowStreaming.parseAndValidateStream(rawDf)

    // Quality summary records oversized message explicitly
    val quality = NetworkFlowStreaming.computeQualitySummary(parsed).head()
    assert(quality.getAs[Long]("total_events") === 3L)
    assert(quality.getAs[Long]("valid_count") === 2L)
    assert(quality.getAs[Long]("malformed_count") === 1L)
    assert(quality.getAs[Long]("oversized_count") === 1L)
    assert(quality.getAs[Long]("benign_count") === 1L)
    assert(quality.getAs[Long]("attack_count") === 1L) // Oversized DDoS must NOT contaminate attack count!

    // Valid stream excludes oversized
    val validStream = parsed.filter(col("is_valid") === true)
    assert(validStream.count() === 2L)

    val binarySummary = NetworkFlowStreaming.computeBinarySummary(validStream).collect()
    val attackRow = binarySummary.find(_.getAs[String]("label_binary") == "ATTACK").get
    assert(attackRow.getAs[Long]("flow_count") === 1L)

    // Dead letter stream captures oversized record
    val deadLetter = NetworkFlowStreaming.deadLetterStream(parsed).collect()
    assert(deadLetter.length === 1)
    assert(deadLetter.head.getAs[Boolean]("is_oversized") === true)
  }

  test("streaming label integrity: preserves UNKNOWN labels and audits unknown_label_count") {
    val unknownLabelJson =
      """{
        |  "flow_id": "flow_unrec",
        |  "timestamp": "2026-10-24 12:00:00",
        |  "destination_port": 80.0,
        |  "flow_duration": 100.0,
        |  "has_invalid_numeric": false,
        |  "label_original": "ZeroDayRansomware",
        |  "label_binary": "UNKNOWN"
        |}""".stripMargin

    val rawDf = makeRawKafkaDf(Seq(
      ("k1", validBenignJson),
      ("k2", unknownLabelJson)
    ))
    val parsed = NetworkFlowStreaming.parseAndValidateStream(rawDf)
    val quality = NetworkFlowStreaming.computeQualitySummary(parsed).head()

    assert(quality.getAs[Long]("total_events") === 2L)
    assert(quality.getAs[Long]("valid_count") === 2L)
    assert(quality.getAs[Long]("benign_count") === 1L)
    assert(quality.getAs[Long]("attack_count") === 0L)
    assert(quality.getAs[Long]("unknown_label_count") === 1L)
  }
}

