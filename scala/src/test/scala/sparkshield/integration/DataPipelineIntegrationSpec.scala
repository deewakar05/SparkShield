package sparkshield.integration

import org.apache.spark.sql.functions._
import org.scalatest.funsuite.AnyFunSuite
import sparkshield.{DataCleaning, DataIngestion, SharedSparkSession, SparkSQLAnalytics}

import java.io.{File, PrintWriter}
import java.nio.file.Files

/**
 * Integration Tests 1 & 2:
 * 1. Ingestion to Cleaning (CSV -> Ingestion -> Cleaning -> Parquet)
 * 2. Cleaning to SQL Analytics (Cleaned Parquet -> SparkSQLAnalytics -> Analytical Outputs)
 *
 * Uses isolated temporary directories and self-contained synthetic datasets.
 */
class DataPipelineIntegrationSpec extends AnyFunSuite with SharedSparkSession {

  private def writeCsv(file: File, header: String, rows: Seq[String]): File = {
    val pw = new PrintWriter(file)
    try {
      pw.println(header)
      rows.foreach(pw.println)
    } finally {
      pw.close()
    }
    file
  }

  // ── Integration Test 1: Ingestion to Cleaning ───────────────

  test("Integration Test 1: CSV -> DataIngestion -> DataCleaning -> Parquet") {
    val tempDir = Files.createTempDirectory("int_test_ingest_clean_").toFile
    val csvFile = new File(tempDir, "input_flows.csv")
    val parquetDir = new File(tempDir, "cleaned_parquet")

    try {
      // 1. Create realistic raw CSV with CICIDS2017 header quirks:
      //    leading spaces, duplicate " Fwd Header Length", mixed case labels, "Infinity"
      val rawHeader = " Destination Port, Flow Duration, Total Fwd Packets, Flow Bytes/s, Fwd Header Length, Fwd Header Length, Label "
      val rawRows = Seq(
        "80, 1000, 5, 2500.0, 40, 40, BENIGN",
        "443, 2000, 10, 5000.0, 80, 80, DDoS",
        "22, 500, 2, 800.0, 20, 20, PortScan",
        "8080, 0, 1, Infinity, 20, 20, DDoS",
        "80, 1500, 4, 3000.0, 32, 32, benign", // lower case benign
        "53, 200, 1, 400.0, 16, 16, Benign"    // capitalized Benign
      )
      writeCsv(csvFile, rawHeader, rawRows)

      // 2. DataIngestion
      val rawDf = DataIngestion.readSingleFile(csvFile.getAbsolutePath, spark)
      assert(rawDf.count() === 6L)
      assert(rawDf.columns.length === 7)

      // 3. DataCleaning
      val cleanResult = DataCleaning.clean(rawDf)
      assert(cleanResult.rowCount === 6L)
      assert(cleanResult.invalidNumerics === 1L) // The row with Infinity
      assert(cleanResult.schema.duplicatesResolved.contains("Fwd Header Length"))

      // Write Parquet
      cleanResult.cleanDf.write.mode("overwrite").parquet(parquetDir.getAbsolutePath)
      assert(new File(parquetDir, "_SUCCESS").exists())

      // 4. Verify Parquet Readability & Integrity
      val parquetDf = spark.read.parquet(parquetDir.getAbsolutePath)
      assert(parquetDf.count() === 6L)

      val cols = parquetDf.columns.toSeq
      assert(cols.contains("destination_port"))
      assert(cols.contains("flow_duration"))
      assert(cols.contains("fwd_header_length"))
      assert(cols.contains("fwd_header_length_2"))
      assert(cols.contains("label_original"))
      assert(cols.contains("label_binary"))
      assert(cols.contains("has_invalid_numeric"))

      // Check binary mapping: 3 BENIGN (BENIGN, benign, Benign) and 3 ATTACK (DDoS, PortScan, DDoS)
      val binaryCounts = parquetDf.groupBy("label_binary").count().collect().map(r => r.getString(0) -> r.getLong(1)).toMap
      assert(binaryCounts("BENIGN") === 3L)
      assert(binaryCounts("ATTACK") === 3L)

      // Check numeric quality: Infinity preserved in Parquet and flagged
      val flaggedRow = parquetDf.filter(parquetDf("has_invalid_numeric") === true).head()
      assert(flaggedRow.getAs[Double]("flow_bytes_s").isInfinite)
    } finally {
      deleteRecursively(tempDir)
    }
  }

  // ── Integration Test 2: Cleaning to SQL Analytics ───────────

  test("Integration Test 2: Cleaned Parquet -> SparkSQLAnalytics -> Analytical Results") {
    val tempDir = Files.createTempDirectory("int_test_clean_sql_").toFile
    val inputParquet = new File(tempDir, "input_parquet")
    val outputResults = new File(tempDir, "sql_results")

    try {
      // 1. Write an independent reference cleaned Parquet dataset:
      //    4 BENIGN, 4 DDoS, 2 PortScan (Total 10)
      val s = spark
      import s.implicits._
      val refData = Seq(
        (80.0, 100.0, 1000.0, 2.0, 2.0, "BENIGN", "BENIGN", false),
        (80.0, 200.0, 2000.0, 3.0, 2.0, "BENIGN", "BENIGN", false),
        (443.0, 300.0, 3000.0, 4.0, 3.0, "BENIGN", "BENIGN", false),
        (443.0, 400.0, 4000.0, 5.0, 4.0, "BENIGN", "BENIGN", false),
        (80.0, 50.0, 500.0, 1.0, 0.0, "DDoS", "ATTACK", false),
        (80.0, 60.0, 600.0, 1.0, 0.0, "DDoS", "ATTACK", false),
        (80.0, 70.0, 700.0, 1.0, 0.0, "DDoS", "ATTACK", false),
        (80.0, 80.0, 800.0, 1.0, 0.0, "DDoS", "ATTACK", false),
        (22.0, 10.0, 100.0, 1.0, 1.0, "PortScan", "ATTACK", false),
        (22.0, 20.0, Double.PositiveInfinity, 1.0, 1.0, "PortScan", "ATTACK", true)
      ).toDF("destination_port", "flow_duration", "flow_bytes_s", "total_fwd_packets", "total_backward_packets", "label_original", "label_binary", "has_invalid_numeric")

      refData.write.mode("overwrite").parquet(inputParquet.getAbsolutePath)

      // 2. Load and run SparkSQLAnalytics
      val loadedDf = SparkSQLAnalytics.loadCleanedData(inputParquet.getAbsolutePath, spark)
      val results = SparkSQLAnalytics.runAnalytics(loadedDf, spark, "int_test_view")

      // 3. Verify exact statistics
      assert(results.totalFlows === 10L)

      // Binary distribution: 4 BENIGN (40%), 6 ATTACK (60%)
      val binaryMap = results.binaryDistribution.collect().map(r => r.getAs[String]("label_binary") -> (r.getAs[Long]("count"), r.getAs[Double]("percentage"))).toMap
      assert(binaryMap("BENIGN")._1 === 4L)
      assert(binaryMap("BENIGN")._2 === 40.0)
      assert(binaryMap("ATTACK")._1 === 6L)
      assert(binaryMap("ATTACK")._2 === 60.0)

      // Top attacks: DDoS (4), PortScan (2)
      val topRows = results.topAttacks.collect()
      assert(topRows.length === 2)
      assert(topRows(0).getAs[String]("label_original") === "DDoS")
      assert(topRows(0).getAs[Long]("attack_count") === 4L)
      assert(topRows(1).getAs[String]("label_original") === "PortScan")
      assert(topRows(1).getAs[Long]("attack_count") === 2L)

      // Feature statistics: flow_bytes_s must have finite avg and max despite Infinity row
      val statsRow = results.featureStats.filter(col("feature_name") === "flow_bytes_s").head()
      assert(!statsRow.getAs[Double]("avg_val").isInfinite)
      assert(!statsRow.getAs[Double]("avg_val").isNaN)
      assert(!statsRow.getAs[Double]("max_val").isInfinite)

      // 4. Save results and verify disk persistence
      SparkSQLAnalytics.saveResults(results, outputResults.getAbsolutePath, inputParquet.getAbsolutePath)
      assert(new File(outputResults, "parquet/binary_distribution/_SUCCESS").exists())
      assert(new File(outputResults, "csv/binary_distribution").exists())
      assert(new File(outputResults, "csv/top_attacks").exists())
      assert(new File(outputResults, "csv/traffic_comparison").exists())

      // 5. Verify CSV outputs contain finite numbers (no "Infinity" in csv)
      val csvTraffic = spark.read.option("header", "true").csv(new File(outputResults, "csv/traffic_comparison").getAbsolutePath)
      val csvRows = csvTraffic.collect()
      assert(csvRows.length === 2)
      csvRows.foreach { r =>
        val avgBytes = r.getAs[String]("avg_flow_bytes_s")
        assert(avgBytes != "Infinity")
        assert(avgBytes != "NaN")
      }
    } finally {
      deleteRecursively(tempDir)
    }
  }
}
