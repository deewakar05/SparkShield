package sparkshield.integration

import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, NewTopic}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.funsuite.AnyFunSuite
import sparkshield.{NetworkFlowProducer, NetworkFlowStreaming, SharedSparkSession}

import java.io.File
import java.nio.file.Files
import java.util.{Collections, Properties}

/**
 * Phase D: End-to-End Pipeline Testing.
 *
 * Deterministic, repeatable end-to-end verification:
 *   Synthetic input -> Producer -> Kafka -> Structured Streaming -> ForeachBatch Persistence -> Independent Verification.
 *
 * Also includes a separate limited demonstration against the cleaned Parquet dataset.
 * Sinks and checkpoints are strictly isolated in temporary directories.
 */
class EndToEndPipelineSpec extends AnyFunSuite with SharedSparkSession {

  private val bootstrapServers = "localhost:9092"
  private var isKafkaAvailable = false

  override def beforeAll(): Unit = {
    super.beforeAll()
    isKafkaAvailable = checkBrokerReachable()
  }

  private def checkBrokerReachable(): Boolean = {
    val props = new Properties()
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "3000")
    props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "3000")
    try {
      val admin = AdminClient.create(props)
      try {
        admin.listTopics().names().get()
        true
      } finally {
        admin.close()
      }
    } catch {
      case _: Exception => false
    }
  }

  private def createTopic(topic: String, partitions: Int): Unit = {
    val props = new Properties()
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    val admin = AdminClient.create(props)
    try {
      admin.createTopics(Collections.singletonList(new NewTopic(topic, partitions, 1.toShort))).all().get()
    } finally {
      admin.close()
    }
  }

  private def deleteTopic(topic: String): Unit = {
    val props = new Properties()
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    val admin = AdminClient.create(props)
    try {
      admin.deleteTopics(Collections.singletonList(topic)).all().get()
    } catch {
      case _: Exception =>
    } finally {
      admin.close()
    }
  }

  // ── 1. Deterministic Synthetic E2E Test ─────────────────────

  test("End-to-End Test: Deterministic 5-event pipeline with exact accounting") {
    assume(isKafkaAvailable, "Local Kafka broker is not available on localhost:9092")

    val topic = s"e2e-synthetic-${System.currentTimeMillis()}"
    val tempDir = Files.createTempDirectory("e2e_synth_test_").toFile
    val checkpointDir = new File(tempDir, "checkpoint")
    val outputDir = new File(tempDir, "output")

    createTopic(topic, partitions = 1)

    try {
      // 1. Synthetic dataset conforming exactly to prompt:
      //    event-001 | BENIGN | 100
      //    event-002 | ATTACK | 200
      //    event-003 | BENIGN | 300
      //    event-004 | ATTACK | 400
      //    event-005 | ATTACK | 500
      val synthSchema = StructType(Seq(
        StructField("flow_id", StringType, nullable = false),
        StructField("timestamp", StringType, nullable = false),
        StructField("destination_port", DoubleType, nullable = true),
        StructField("flow_duration", DoubleType, nullable = true),
        StructField("flow_bytes_s", DoubleType, nullable = true),
        StructField("flow_packets_s", DoubleType, nullable = true),
        StructField("total_fwd_packets", DoubleType, nullable = true),
        StructField("total_backward_packets", DoubleType, nullable = true),
        StructField("has_invalid_numeric", BooleanType, nullable = true),
        StructField("label_original", StringType, nullable = true),
        StructField("label_binary", StringType, nullable = false)
      ))

      val events = Seq(
        Row("event-001", "2026-10-24 12:00:00", 80.0,  100.0, 1000.0, 10.0, 1.0, 1.0, false, "BENIGN", "BENIGN"),
        Row("event-002", "2026-10-24 12:00:10", 443.0, 200.0, 2000.0, 20.0, 2.0, 2.0, false, "DDoS",   "ATTACK"),
        Row("event-003", "2026-10-24 12:00:20", 80.0,  300.0, 3000.0, 30.0, 3.0, 3.0, false, "BENIGN", "BENIGN"),
        Row("event-004", "2026-10-24 12:00:30", 443.0, 400.0, 4000.0, 40.0, 4.0, 4.0, false, "PortScan","ATTACK"),
        Row("event-005", "2026-10-24 12:00:40", 22.0,  500.0, 5000.0, 50.0, 5.0, 5.0, false, "Botnet", "ATTACK")
      )
      val inputDf = spark.createDataFrame(spark.sparkContext.parallelize(events), synthSchema)

      // 2. Format to JSON payloads
      val structCols = inputDf.columns.map(col)
      val jsonDf = inputDf.select(
        col("flow_id").as("key"),
        to_json(struct(structCols: _*)).as("value")
      )

      // 3. Publish to Kafka
      val publishedCount = NetworkFlowProducer.publishBatch(jsonDf, bootstrapServers, topic)
      assert(publishedCount === 5L)

      // 4. Ingest via Structured Streaming
      val rawStream = NetworkFlowStreaming.readKafkaStream(spark, bootstrapServers, topic, "earliest")
      val parsedStream = NetworkFlowStreaming.parseAndValidateStream(rawStream)
      val validStream = parsedStream.filter(col("is_valid") === true)

      val query = validStream.writeStream
        .foreachBatch { (batchDf: DataFrame, batchId: Long) =>
          NetworkFlowStreaming.processMicroBatch(batchDf, batchId, outputDir.getAbsolutePath)
        }
        .option("checkpointLocation", checkpointDir.getAbsolutePath)
        .start()

      query.processAllAvailable()
      query.stop()

      // 5. Independently verify persisted results in Parquet & CSV
      val parquetBatch0 = new File(outputDir, "parquet/batch_0")
      val csvBatch0 = new File(outputDir, "csv/batch_0")

      assert(parquetBatch0.exists())
      assert(csvBatch0.exists())

      val resultDf = spark.read.parquet(parquetBatch0.getAbsolutePath)
      assert(resultDf.count() === 5L)

      // Check distribution: Total 5, BENIGN 2 (40%), ATTACK 3 (60%)
      val benignCount = resultDf.filter(upper(col("label_binary")) === "BENIGN").count()
      val attackCount = resultDf.filter(upper(col("label_binary")) === "ATTACK").count()
      assert(benignCount === 2L)
      assert(attackCount === 3L)

      val benignPct = (benignCount * 100.0) / 5.0
      val attackPct = (attackCount * 100.0) / 5.0
      assert(benignPct === 40.0)
      assert(attackPct === 60.0)

      // Verify all flow IDs
      val ids = resultDf.select("flow_id").collect().map(_.getString(0)).toSet
      assert(ids === Set("event-001", "event-002", "event-003", "event-004", "event-005"))

      // Verify average flow duration
      val avgDuration = resultDf.agg(avg(col("flow_duration"))).head().getDouble(0)
      assert(avgDuration === 300.0) // (100 + 200 + 300 + 400 + 500) / 5 = 300.0
    } finally {
      deleteTopic(topic)
      deleteRecursively(tempDir)
    }
  }

  // ── 2. Real Cleaned Dataset E2E Demonstration ──────────────

  test("End-to-End Test: Demonstration against actual cleaned Parquet dataset") {
    assume(isKafkaAvailable, "Local Kafka broker is not available on localhost:9092")

    val realParquetPath = Seq("data/processed/cleaned_2026-10-24", "../data/processed/cleaned_2026-10-24")
      .find(p => new File(p).exists())
      .getOrElse("data/processed/cleaned_2026-10-24")
    assume(new File(realParquetPath).exists(), s"Parquet dataset not found at $realParquetPath")

    val topic = s"e2e-demo-${System.currentTimeMillis()}"
    val tempDir = Files.createTempDirectory("e2e_demo_sink_").toFile
    val checkpointDir = new File(tempDir, "checkpoint")
    val outputDir = new File(tempDir, "output")

    createTopic(topic, partitions = 2)

    try {
      // 1. Load stratified sample of 10 records (5 BENIGN, 5 ATTACK) from actual cleaned dataset
      val preparedDf = NetworkFlowProducer.loadAndPrepare(
        inputPath  = realParquetPath,
        maxRecords = Some(10L),
        spark      = spark,
        stratified = true
      )
      assert(preparedDf.count() === 10L)

      // 2. Publish to Kafka
      val published = NetworkFlowProducer.publishBatch(preparedDf, bootstrapServers, topic)
      assert(published === 10L)

      // 3. Consume via Structured Streaming
      val rawStream = NetworkFlowStreaming.readKafkaStream(spark, bootstrapServers, topic, "earliest")
      val parsedStream = NetworkFlowStreaming.parseAndValidateStream(rawStream)
      val validStream = parsedStream.filter(col("is_valid") === true)

      val query = validStream.writeStream
        .foreachBatch { (batchDf: DataFrame, batchId: Long) =>
          NetworkFlowStreaming.processMicroBatch(batchDf, batchId, outputDir.getAbsolutePath)
        }
        .option("checkpointLocation", checkpointDir.getAbsolutePath)
        .start()

      query.processAllAvailable()
      query.stop()

      // 4. Verify output
      val parquetBatch0 = new File(outputDir, "parquet/batch_0")
      assert(parquetBatch0.exists())
      val resultDf = spark.read.parquet(parquetBatch0.getAbsolutePath)
      assert(resultDf.count() === 10L)

      val benign = resultDf.filter(upper(col("label_binary")) === "BENIGN").count()
      val attack = resultDf.filter(upper(col("label_binary")) === "ATTACK").count()
      assert(benign === 5L)
      assert(attack === 5L)
    } finally {
      deleteTopic(topic)
      deleteRecursively(tempDir)
    }
  }
}
