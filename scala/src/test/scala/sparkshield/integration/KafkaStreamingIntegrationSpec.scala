package sparkshield.integration

import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, NewTopic}
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.funsuite.AnyFunSuite
import sparkshield.{NetworkFlowProducer, NetworkFlowStreaming, SharedSparkSession}

import java.io.File
import java.nio.file.Files
import java.time.Duration
import java.util.{Collections, Properties}

/**
 * Integration Tests 3, 4, & 5:
 * - Test 3: Kafka producer to broker (publish, verify topic contents, schema, labels)
 * - Test 4: Broker to Structured Streaming (ingest, parse, sanitize, aggregate, sink)
 * - Test 5: Restart & Checkpoint Recovery (batch 1, stop, batch 2, restart, verify idempotency)
 *
 * Uses isolated temporary Kafka topics and isolated checkpoint directories.
 * Does NOT alter or consume the main 'network-flows' topic.
 */
class KafkaStreamingIntegrationSpec extends AnyFunSuite with SharedSparkSession {

  private val bootstrapServers = "localhost:9092"
  private var isKafkaAvailable = false

  override def beforeAll(): Unit = {
    super.beforeAll()
    // Check if Kafka broker is reachable
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
      case _: Exception => // ignore cleanup errors
    } finally {
      admin.close()
    }
  }

  private val testSchema = StructType(Seq(
    StructField("destination_port", DoubleType, nullable = true),
    StructField("flow_duration", DoubleType, nullable = true),
    StructField("flow_bytes_s", DoubleType, nullable = true),
    StructField("total_fwd_packets", DoubleType, nullable = true),
    StructField("total_backward_packets", DoubleType, nullable = true),
    StructField("label_original", StringType, nullable = true),
    StructField("label_binary", StringType, nullable = false),
    StructField("has_invalid_numeric", BooleanType, nullable = true)
  ))

  // ── Integration Test 3: Producer to Broker ──────────────────

  test("Integration Test 3: Kafka producer publishes valid JSON messages to broker topic") {
    assume(isKafkaAvailable, "Local Kafka broker is not available on localhost:9092")

    val topic = s"int-test-producer-${System.currentTimeMillis()}"
    createTopic(topic, partitions = 2)

    try {
      // 1. Prepare synthetic dataset: 3 BENIGN and 3 ATTACK
      val rows = Seq(
        Row(80.0, 100.0, 1000.0, 1.0, 1.0, "BENIGN", "BENIGN", false),
        Row(80.0, 200.0, 2000.0, 2.0, 2.0, "BENIGN", "BENIGN", false),
        Row(80.0, 300.0, 3000.0, 3.0, 3.0, "BENIGN", "BENIGN", false),
        Row(443.0, 50.0, 500.0, 1.0, 0.0, "DDoS", "ATTACK", false),
        Row(443.0, 60.0, 600.0, 1.0, 0.0, "DDoS", "ATTACK", false),
        Row(22.0, 70.0, 700.0, 1.0, 0.0, "PortScan", "ATTACK", false)
      )
      val df = spark.createDataFrame(spark.sparkContext.parallelize(rows), testSchema)
      val jsonDf = NetworkFlowProducer.prepareJsonDataFrame(df)

      // 2. Publish to Kafka
      val publishedCount = NetworkFlowProducer.publishStreamingReplay(
        jsonDf          = jsonDf,
        bootstrapServers= bootstrapServers,
        topic           = topic,
        delayMs         = 0L,
        spark           = spark
      )
      assert(publishedCount === 6L)

      // 3. Consume directly from Kafka to verify topic records
      val consumerProps = new Properties()
      consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
      consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, s"int-test-group-${System.currentTimeMillis()}")
      consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
      consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
      consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

      val consumer = new KafkaConsumer[String, String](consumerProps)
      try {
        consumer.subscribe(Collections.singletonList(topic))
        val records = new scala.collection.mutable.ArrayBuffer[(String, String)]()
        val deadline = System.currentTimeMillis() + 10000L

        while (records.size < 6 && System.currentTimeMillis() < deadline) {
          val polled = consumer.poll(Duration.ofMillis(500))
          val iter = polled.iterator()
          while (iter.hasNext) {
            val r = iter.next()
            records += (r.key() -> r.value())
          }
        }

        assert(records.size === 6)
        assert(records.forall(_._1.startsWith("flow_")))
        assert(records.exists(_._2.contains("\"label_original\":\"BENIGN\"")))
        assert(records.exists(_._2.contains("\"label_original\":\"DDoS\"")))
        assert(records.exists(_._2.contains("\"label_original\":\"PortScan\"")))
      } finally {
        consumer.close()
      }
    } finally {
      deleteTopic(topic)
    }
  }

  // ── Integration Test 4: Broker to Structured Streaming ───────

  test("Integration Test 4: Broker to Structured Streaming consumer pipeline") {
    assume(isKafkaAvailable, "Local Kafka broker is not available on localhost:9092")

    val topic = s"int-test-streaming-${System.currentTimeMillis()}"
    val tempDir = Files.createTempDirectory("int_test_streaming_sink_").toFile
    val checkpointDir = new File(tempDir, "checkpoint")
    val outputDir = new File(tempDir, "output")

    createTopic(topic, partitions = 2)

    try {
      // 1. Publish 4 valid events (2 BENIGN, 2 ATTACK) + 1 malformed JSON to Kafka
      val validBenign =
        """{"flow_id":"flow_1","timestamp":"2026-10-24 12:00:01","destination_port":80.0,"flow_duration":100.0,"flow_bytes_s":1000.0,"total_fwd_packets":1.0,"total_backward_packets":1.0,"has_invalid_numeric":false,"label_original":"BENIGN","label_binary":"BENIGN"}"""
      val validBenign2 =
        """{"flow_id":"flow_2","timestamp":"2026-10-24 12:00:02","destination_port":80.0,"flow_duration":200.0,"flow_bytes_s":2000.0,"total_fwd_packets":2.0,"total_backward_packets":2.0,"has_invalid_numeric":false,"label_original":"BENIGN","label_binary":"BENIGN"}"""
      val validAttack =
        """{"flow_id":"flow_3","timestamp":"2026-10-24 12:00:03","destination_port":443.0,"flow_duration":300.0,"flow_bytes_s":3000.0,"total_fwd_packets":3.0,"total_backward_packets":3.0,"has_invalid_numeric":false,"label_original":"DDoS","label_binary":"ATTACK"}"""
      val validAttackInf =
        """{"flow_id":"flow_4","timestamp":"2026-10-24 12:00:04","destination_port":443.0,"flow_duration":400.0,"flow_bytes_s":"Infinity","total_fwd_packets":4.0,"total_backward_packets":4.0,"has_invalid_numeric":true,"label_original":"DDoS","label_binary":"ATTACK"}"""
      val malformed = "{ broken_json_payload: null "

      val kafkaRecords = Seq(
        Row("k1", validBenign),
        Row("k2", validBenign2),
        Row("k3", validAttack),
        Row("k4", validAttackInf),
        Row("k5", malformed)
      )
      val kafkaInputSchema = StructType(Seq(StructField("key", StringType), StructField("value", StringType)))
      val publishDf = spark.createDataFrame(spark.sparkContext.parallelize(kafkaRecords), kafkaInputSchema)
      NetworkFlowProducer.publishBatch(publishDf, bootstrapServers, topic)

      // 2. Read stream from Kafka topic
      val rawStream = NetworkFlowStreaming.readKafkaStream(
        spark           = spark,
        bootstrapServers= bootstrapServers,
        topic           = topic,
        startingOffsets = "earliest"
      )

      val parsed = NetworkFlowStreaming.parseAndValidateStream(rawStream)
      val validFlows = parsed.filter(col("is_valid") === true)

      // 3. Run micro-batch persistence query
      val query = validFlows.writeStream
        .foreachBatch { (batchDf: DataFrame, batchId: Long) =>
          NetworkFlowStreaming.processMicroBatch(batchDf, batchId, outputDir.getAbsolutePath)
        }
        .option("checkpointLocation", checkpointDir.getAbsolutePath)
        .start()

      // Process available data with timeout
      query.processAllAvailable()
      query.stop()

      // 4. Verify outputs: Exactly 4 valid records written to batch_0
      val batch0Parquet = new File(outputDir, "parquet/batch_0")
      assert(batch0Parquet.exists())
      val parquetDf = spark.read.parquet(batch0Parquet.getAbsolutePath)
      assert(parquetDf.count() === 4L)

      val benignCount = parquetDf.filter(upper(col("label_binary")) === "BENIGN").count()
      val attackCount = parquetDf.filter(upper(col("label_binary")) === "ATTACK").count()
      assert(benignCount === 2L)
      assert(attackCount === 2L)

      // Malformed event (broken JSON) was filtered out and not written
      assert(parquetDf.filter(col("flow_id") === "flow_1").count() === 1L)
      assert(parquetDf.filter(col("has_invalid_numeric") === true).count() === 1L)
    } finally {
      deleteTopic(topic)
      deleteRecursively(tempDir)
    }
  }

  // ── Integration Test 5: Checkpoint Recovery & Offset Resumption ──

  test("Integration Test 5: Checkpoint recovery resumes offsets without duplicate re-processing") {
    assume(isKafkaAvailable, "Local Kafka broker is not available on localhost:9092")

    val topic = s"int-test-recovery-${System.currentTimeMillis()}"
    val tempDir = Files.createTempDirectory("int_test_recovery_").toFile
    val checkpointDir = new File(tempDir, "checkpoint")
    val outputDir = new File(tempDir, "output")

    createTopic(topic, partitions = 1)

    try {
      val kafkaInputSchema = StructType(Seq(StructField("key", StringType), StructField("value", StringType)))

      // ── Step A: Produce Batch 1 (2 records) ─────────────────
      val batch1Events = Seq(
        Row("b1_1", """{"flow_id":"b1_1","timestamp":"2026-10-24 12:00:01","destination_port":80.0,"flow_duration":100.0,"flow_bytes_s":1000.0,"total_fwd_packets":1.0,"total_backward_packets":1.0,"has_invalid_numeric":false,"label_original":"BENIGN","label_binary":"BENIGN"}"""),
        Row("b1_2", """{"flow_id":"b1_2","timestamp":"2026-10-24 12:00:02","destination_port":80.0,"flow_duration":200.0,"flow_bytes_s":2000.0,"total_fwd_packets":2.0,"total_backward_packets":2.0,"has_invalid_numeric":false,"label_original":"BENIGN","label_binary":"BENIGN"}""")
      )
      val b1Df = spark.createDataFrame(spark.sparkContext.parallelize(batch1Events), kafkaInputSchema)
      NetworkFlowProducer.publishBatch(b1Df, bootstrapServers, topic)

      // ── Step B: Start Consumer Query 1 with fresh checkpoint ──
      val rawStream1 = NetworkFlowStreaming.readKafkaStream(spark, bootstrapServers, topic, "earliest")
      val valid1 = NetworkFlowStreaming.parseAndValidateStream(rawStream1).filter(col("is_valid") === true)

      val query1 = valid1.writeStream
        .foreachBatch { (batchDf: DataFrame, batchId: Long) =>
          NetworkFlowStreaming.processMicroBatch(batchDf, batchId, outputDir.getAbsolutePath)
        }
        .option("checkpointLocation", checkpointDir.getAbsolutePath)
        .start()

      query1.processAllAvailable()
      query1.stop()

      // Verify batch 0 persisted
      val batch0Parquet = new File(outputDir, "parquet/batch_0")
      assert(batch0Parquet.exists())
      assert(spark.read.parquet(batch0Parquet.getAbsolutePath).count() === 2L)

      // ── Step C: Produce Batch 2 (3 records) while consumer is STOPPED ──
      val batch2Events = Seq(
        Row("b2_1", """{"flow_id":"b2_1","timestamp":"2026-10-24 12:01:01","destination_port":443.0,"flow_duration":300.0,"flow_bytes_s":3000.0,"total_fwd_packets":3.0,"total_backward_packets":3.0,"has_invalid_numeric":false,"label_original":"DDoS","label_binary":"ATTACK"}"""),
        Row("b2_2", """{"flow_id":"b2_2","timestamp":"2026-10-24 12:01:02","destination_port":443.0,"flow_duration":400.0,"flow_bytes_s":4000.0,"total_fwd_packets":4.0,"total_backward_packets":4.0,"has_invalid_numeric":false,"label_original":"DDoS","label_binary":"ATTACK"}"""),
        Row("b2_3", """{"flow_id":"b2_3","timestamp":"2026-10-24 12:01:03","destination_port":22.0,"flow_duration":500.0,"flow_bytes_s":5000.0,"total_fwd_packets":5.0,"total_backward_packets":5.0,"has_invalid_numeric":false,"label_original":"PortScan","label_binary":"ATTACK"}""")
      )
      val b2Df = spark.createDataFrame(spark.sparkContext.parallelize(batch2Events), kafkaInputSchema)
      NetworkFlowProducer.publishBatch(b2Df, bootstrapServers, topic)

      // ── Step D: Restart Consumer Query 2 using the SAME checkpoint ──
      val rawStream2 = NetworkFlowStreaming.readKafkaStream(spark, bootstrapServers, topic, "earliest")
      val valid2 = NetworkFlowStreaming.parseAndValidateStream(rawStream2).filter(col("is_valid") === true)

      val query2 = valid2.writeStream
        .foreachBatch { (batchDf: DataFrame, batchId: Long) =>
          NetworkFlowStreaming.processMicroBatch(batchDf, batchId, outputDir.getAbsolutePath)
        }
        .option("checkpointLocation", checkpointDir.getAbsolutePath)
        .start()

      query2.processAllAvailable()
      query2.stop()

      // ── Step E: Verify Checkpoint Recovery Guarantees ────────
      // 1. Batch 0 was NOT reprocessed or duplicated
      val batch0Check = spark.read.parquet(batch0Parquet.getAbsolutePath)
      assert(batch0Check.count() === 2L)

      // 2. Batch 1 was processed with exactly the 3 new records from Batch 2
      val batch1Parquet = new File(outputDir, "parquet/batch_1")
      assert(batch1Parquet.exists())
      val batch1Check = spark.read.parquet(batch1Parquet.getAbsolutePath)
      assert(batch1Check.count() === 3L)

      // Verify no overlap between batch 0 and batch 1 records
      val b0Ids = batch0Check.select("flow_id").collect().map(_.getString(0)).toSet
      val b1Ids = batch1Check.select("flow_id").collect().map(_.getString(0)).toSet
      assert(b0Ids.intersect(b1Ids).isEmpty)
    } finally {
      deleteTopic(topic)
      deleteRecursively(tempDir)
    }
  }

  // ── Integration Test 6: Sink Retry & Failure Recovery Idempotency ──

  test("Integration Test 6: Simulated recoverable sink retry preserves idempotency without duplicate data") {
    val tempDir = Files.createTempDirectory("int_test_sink_retry_").toFile
    try {
      val schema = StructType(Seq(
        StructField("flow_id", StringType, nullable = true),
        StructField("timestamp", StringType, nullable = true),
        StructField("destination_port", DoubleType, nullable = true),
        StructField("flow_duration", DoubleType, nullable = true),
        StructField("total_fwd_packets", DoubleType, nullable = true),
        StructField("total_backward_packets", DoubleType, nullable = true),
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

      val batchRows = Seq(
        Row("retry_1", "2026-10-24 12:00:00", 80.0, 100.0, 1.0, 1.0, 1000.0, 10.0, 500.0, 500.0, 20.0, 20.0, false, "BENIGN", "BENIGN"),
        Row("retry_2", "2026-10-24 12:00:01", 443.0, 200.0, 2.0, 2.0, 2000.0, 20.0, 600.0, 600.0, 20.0, 20.0, false, "DDoS", "ATTACK")
      )
      val batchDf = spark.createDataFrame(spark.sparkContext.parallelize(batchRows), schema)

      // Initial execution of batch 99
      NetworkFlowStreaming.processMicroBatch(batchDf, batchId = 99L, tempDir.getAbsolutePath)
      val p1 = spark.read.parquet(new File(tempDir, "parquet/batch_99").getAbsolutePath)
      val c1 = spark.read.option("header", "true").csv(new File(tempDir, "csv/batch_99").getAbsolutePath)
      assert(p1.count() === 2L)
      assert(c1.count() === 2L)

      // Simulate executor crash / recoverable failure retry with identical batchId 99
      NetworkFlowStreaming.processMicroBatch(batchDf, batchId = 99L, tempDir.getAbsolutePath)
      val p2 = spark.read.parquet(new File(tempDir, "parquet/batch_99").getAbsolutePath)
      val c2 = spark.read.option("header", "true").csv(new File(tempDir, "csv/batch_99").getAbsolutePath)

      // Exactly-once file output guarantee: overwrite mode prevents double counting
      assert(p2.count() === 2L, "Retried batch must overwrite, not append duplicate records")
      assert(c2.count() === 2L, "Retried CSV batch must overwrite, not duplicate CSV rows")
    } finally {
      deleteRecursively(tempDir)
    }
  }

  // ── Integration Test 7: Missing Checkpoint Graceful Initialization ──

  test("Integration Test 7: Missing checkpoint location initializes cleanly without crashing") {
    assume(isKafkaAvailable, "Local Kafka broker is not available on localhost:9092")

    val topic = s"int-test-missing-ckpt-${System.currentTimeMillis()}"
    val tempDir = Files.createTempDirectory("int_test_missing_ckpt_").toFile
    val nonExistentCkpt = new File(tempDir, "brand_new_checkpoint_dir")
    val outputDir = new File(tempDir, "output")

    createTopic(topic, partitions = 1)
    try {
      assert(!nonExistentCkpt.exists(), "Checkpoint directory should not exist prior to query start")

      val rawStream = NetworkFlowStreaming.readKafkaStream(spark, bootstrapServers, topic, "earliest")
      val validStream = NetworkFlowStreaming.parseAndValidateStream(rawStream).filter(col("is_valid") === true)

      val query = validStream.writeStream
        .foreachBatch { (batchDf: DataFrame, batchId: Long) =>
          NetworkFlowStreaming.processMicroBatch(batchDf, batchId, outputDir.getAbsolutePath)
        }
        .option("checkpointLocation", nonExistentCkpt.getAbsolutePath)
        .start()

      query.processAllAvailable()
      query.stop()

      // Spark Structured Streaming creates checkpoint structure automatically
      assert(nonExistentCkpt.exists(), "Checkpoint directory should be created cleanly by streaming query")
    } finally {
      deleteTopic(topic)
      deleteRecursively(tempDir)
    }
  }
}
