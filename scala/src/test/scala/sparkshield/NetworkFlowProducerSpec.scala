package sparkshield

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.scalatest.funsuite.AnyFunSuite

import java.io.File
import java.nio.file.Files

/**
 * Unit tests for NetworkFlowProducer (Milestone 4).
 *
 * Verifies CLI argument parsing, JSON schema serialization,
 * stratified sampling behavior, event ID generation, and resource handling
 * without requiring a live Kafka broker.
 */
class NetworkFlowProducerSpec extends AnyFunSuite with SharedSparkSession {

  private val testSchema = StructType(Seq(
    StructField("destination_port", DoubleType, nullable = true),
    StructField("flow_duration", DoubleType, nullable = true),
    StructField("flow_bytes_s", DoubleType, nullable = true),
    StructField("label_original", StringType, nullable = true),
    StructField("label_binary", StringType, nullable = false),
    StructField("has_invalid_numeric", BooleanType, nullable = true)
  ))

  // ── 1. CLI Argument Parsing ─────────────────────────────────

  test("parseArgs parses default configuration when no args are provided") {
    val config = NetworkFlowProducer.parseArgs(Array.empty)
    assert(config.topic === NetworkFlowProducer.DefaultTopic)
    assert(config.bootstrapServers === NetworkFlowProducer.DefaultBootstrapServers)
    assert(config.inputPath === NetworkFlowProducer.DefaultInputPath)
    assert(config.maxRecords === Some(5000L))
    assert(config.delayMs === 2L)
    assert(config.stratified === true)
  }

  test("parseArgs parses custom CLI flags accurately") {
    val args = Array(
      "--input", "custom/data/path",
      "--servers", "kafka-broker:9094",
      "--topic", "custom-topic",
      "--max-records", "1200",
      "--delay-ms", "10",
      "--stratified", "false"
    )
    val config = NetworkFlowProducer.parseArgs(args)
    assert(config.inputPath === "custom/data/path")
    assert(config.bootstrapServers === "kafka-broker:9094")
    assert(config.topic === "custom-topic")
    assert(config.maxRecords === Some(1200L))
    assert(config.delayMs === 10L)
    assert(config.stratified === false)
  }

  test("parseArgs parses --max-records all as None") {
    val args = Array("--max-records", "all")
    val config = NetworkFlowProducer.parseArgs(args)
    assert(config.maxRecords === None)
  }

  test("parseArgs parses --sequential flag as stratified=false") {
    val args = Array("--sequential")
    val config = NetworkFlowProducer.parseArgs(args)
    assert(config.stratified === false)
  }

  // ── 2. JSON Serialization & Metadata Generation ─────────────

  test("prepareJsonDataFrame produces unique flow_ids and valid JSON payloads") {
    val rows = Seq(
      Row(80.0, 100.0, 1500.0, "BENIGN", "BENIGN", false),
      Row(443.0, 200.0, 3000.0, "DDoS", "ATTACK", false),
      Row(22.0, 50.0, 800.0, "PortScan", "ATTACK", false)
    )
    val df = spark.createDataFrame(spark.sparkContext.parallelize(rows), testSchema)

    val jsonDf = NetworkFlowProducer.prepareJsonDataFrame(df)
    assert(jsonDf.columns.toSeq === Seq("key", "value"))

    val collected = jsonDf.collect()
    assert(collected.length === 3)

    // Keys must be unique flow_ids
    val keys = collected.map(_.getAs[String]("key"))
    assert(keys.distinct.length === 3)
    assert(keys.forall(_.startsWith("flow_")))

    // Values must be valid JSON strings with all fields
    collected.foreach { r =>
      val json = r.getAs[String]("value")
      assert(json.contains("\"flow_id\""))
      assert(json.contains("\"timestamp\""))
      assert(json.contains("\"destination_port\""))
      assert(json.contains("\"label_original\""))
      assert(json.contains("\"label_binary\""))
    }
  }

  test("prepareJsonDataFrame handles empty DataFrame without error") {
    val emptyDf = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], testSchema)
    val jsonDf = NetworkFlowProducer.prepareJsonDataFrame(emptyDf)
    assert(jsonDf.count() === 0L)
    assert(jsonDf.columns.toSeq === Seq("key", "value"))
  }

  // ── 3. Stratified vs Sequential Sampling ─────────────────────

  test("loadAndPrepare with stratified=true balances BENIGN and ATTACK traffic") {
    val tempDir = Files.createTempDirectory("stratified_test_").toFile
    try {
      // Create dataset with 10 BENIGN and 10 ATTACK flows
      val rows = (1 to 10).map(i => Row(80.0, i * 10.0, 1000.0, "BENIGN", "BENIGN", false)) ++
                 (1 to 10).map(i => Row(443.0, i * 20.0, 2000.0, "DDoS", "ATTACK", false))
      val df = spark.createDataFrame(spark.sparkContext.parallelize(rows), testSchema)
      df.write.mode("overwrite").parquet(tempDir.getAbsolutePath)

      val preparedDf = NetworkFlowProducer.loadAndPrepare(
        inputPath  = tempDir.getAbsolutePath,
        maxRecords = Some(6L),
        spark      = spark,
        stratified = true
      )

      assert(preparedDf.count() === 6L)

      // Parse payload to check balance: 3 BENIGN and 3 ATTACK
      val parsed = NetworkFlowStreaming.parseAndValidateStream(preparedDf)
      val benignCount = parsed.filter(upper(col("label_binary")) === "BENIGN").count()
      val attackCount = parsed.filter(upper(col("label_binary")) === "ATTACK").count()

      assert(benignCount === 3L)
      assert(attackCount === 3L)
    } finally {
      deleteRecursively(tempDir)
    }
  }

  test("loadAndPrepare throws IllegalArgumentException for non-existent path") {
    assertThrows[IllegalArgumentException] {
      NetworkFlowProducer.loadAndPrepare("/non/existent/path/data.parquet", Some(10L), spark)
    }
  }

  // ── 4. SEC-05 Producer Payload Limit Validation ─────────────

  test("SEC-05: filterValidPayloads excludes payloads exceeding maxPayloadBytes") {
    val schema = org.apache.spark.sql.types.StructType(Seq(
      org.apache.spark.sql.types.StructField("key", org.apache.spark.sql.types.StringType),
      org.apache.spark.sql.types.StructField("value", org.apache.spark.sql.types.StringType)
    ))

    val normalPayload = """{"flow_id":"f1","data":"normal"}"""
    val oversizedPayload = """{"flow_id":"f2","data":"""" + ("x" * 70000) + """"}"""

    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row("k1", normalPayload),
        Row("k2", oversizedPayload)
      )),
      schema
    )

    val filtered = NetworkFlowProducer.filterValidPayloads(df, maxPayloadBytes = 65536L)
    assert(filtered.count() === 1L)
    assert(filtered.head().getAs[String]("key") === "k1")
  }

  test("SEC-05: filterValidPayloads correctly measures UTF-8 byte length for non-ASCII strings") {
    val schema = org.apache.spark.sql.types.StructType(Seq(
      org.apache.spark.sql.types.StructField("key", org.apache.spark.sql.types.StringType),
      org.apache.spark.sql.types.StructField("value", org.apache.spark.sql.types.StringType)
    ))

    // 25,000 Chinese characters: 25,000 chars (<64k chars), but 75,000 bytes (>64k bytes)
    val chinesePayload = """{"flow_id":"f_cn","data":"""" + ("\u4e2d" * 25000) + """"}"""
    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row("k_cn", chinesePayload))),
      schema
    )

    val filtered = NetworkFlowProducer.filterValidPayloads(df, maxPayloadBytes = 65536L)
    assert(filtered.count() === 0L) // Must be excluded because UTF-8 bytes > 65536
  }
}
