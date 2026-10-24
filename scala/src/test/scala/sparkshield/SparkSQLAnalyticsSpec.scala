package sparkshield

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite

import java.io.File
import java.nio.file.Files

/**
 * Unit tests for SparkSQLAnalytics (Milestone 3).
 *
 * Uses small synthetic DataFrames and temporary directories.
 * Does not require or hardcode the full CICIDS2017 dataset.
 */
class SparkSQLAnalyticsSpec extends AnyFunSuite with SharedSparkSession {

  // ── Fixture Helpers ─────────────────────────────────────────

  private val testSchema = StructType(Seq(
    StructField("destination_port", DoubleType, nullable = true),
    StructField("flow_duration", DoubleType, nullable = true),
    StructField("flow_bytes_s", DoubleType, nullable = true),
    StructField("label_original", StringType, nullable = true),
    StructField("label_binary", StringType, nullable = false),
    StructField("has_invalid_numeric", BooleanType, nullable = true)
  ))

  /** Create a small synthetic dataset: 2 BENIGN, 3 DDoS, 1 PortScan (Total 6) */
  private def makeSyntheticDf() = {
    val data = Seq(
      Row(80.0,   100.0, 5000.0, "BENIGN",   "BENIGN", false),
      Row(443.0,  200.0, 8000.0, "BENIGN",   "BENIGN", false),
      Row(80.0,    50.0, 2000.0, "DDoS",     "ATTACK", false),
      Row(80.0,    60.0, 2400.0, "DDoS",     "ATTACK", false),
      Row(80.0,    40.0, 1600.0, "DDoS",     "ATTACK", false),
      Row(22.0,   300.0,  900.0, "PortScan", "ATTACK", false)
    )
    spark.createDataFrame(spark.sparkContext.parallelize(data), testSchema)
  }

  /** Create an empty DataFrame with valid schema */
  private def makeEmptyDf() = {
    spark.createDataFrame(spark.sparkContext.emptyRDD[Row], testSchema)
  }

  // ── 1. Schema Validation Tests ──────────────────────────────

  test("validateSchema passes when all required columns exist") {
    val df = makeSyntheticDf()
    SparkSQLAnalytics.validateSchema(df)
  }

  test("validateSchema throws IllegalArgumentException when required columns are missing") {
    val badSchema = StructType(Seq(
      StructField("destination_port", DoubleType, nullable = true),
      StructField("flow_duration", DoubleType, nullable = true)
    ))
    val badDf = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], badSchema)

    val ex = intercept[IllegalArgumentException] {
      SparkSQLAnalytics.validateSchema(badDf)
    }
    assert(ex.getMessage.contains("missing required columns"))
    assert(ex.getMessage.contains("label_binary"))
    assert(ex.getMessage.contains("label_original"))
    assert(ex.getMessage.contains("has_invalid_numeric"))
  }

  // ── 2. Total Flow Count ─────────────────────────────────────

  test("queryTotalFlows returns correct count on synthetic dataset") {
    val df = makeSyntheticDf()
    SparkSQLAnalytics.registerTempView(df, "synth_total")
    val total = SparkSQLAnalytics.queryTotalFlows(spark, "synth_total")
    assert(total === 6L)
  }

  test("queryTotalFlows returns 0 on empty dataset") {
    val df = makeEmptyDf()
    SparkSQLAnalytics.registerTempView(df, "synth_empty")
    val total = SparkSQLAnalytics.queryTotalFlows(spark, "synth_empty")
    assert(total === 0L)
  }

  // ── 3. Binary Label Distribution ────────────────────────────

  test("queryBinaryDistribution computes exact counts and percentages") {
    val df = makeSyntheticDf()
    SparkSQLAnalytics.registerTempView(df, "synth_binary")
    val distDf = SparkSQLAnalytics.queryBinaryDistribution(spark, "synth_binary")

    val rows = distDf.collect()
    assert(rows.length === 2)

    val resultMap = rows.map(r => r.getAs[String]("label_binary") -> (r.getAs[Long]("count"), r.getAs[Double]("percentage"))).toMap

    // 4 ATTACK out of 6 = 66.6667%
    val (attackCount, attackPct) = resultMap("ATTACK")
    assert(attackCount === 4L)
    assert(math.abs(attackPct - 66.6667) < 0.01)

    // 2 BENIGN out of 6 = 33.3333%
    val (benignCount, benignPct) = resultMap("BENIGN")
    assert(benignCount === 2L)
    assert(math.abs(benignPct - 33.3333) < 0.01)
  }

  test("queryBinaryDistribution handles empty dataset safely without division by zero") {
    val df = makeEmptyDf()
    SparkSQLAnalytics.registerTempView(df, "synth_empty_binary")
    val distDf = SparkSQLAnalytics.queryBinaryDistribution(spark, "synth_empty_binary")

    assert(distDf.count() === 0L)
    assert(distDf.columns.toSeq === Seq("label_binary", "count", "percentage"))
  }

  // ── 4. Original Label Distribution ──────────────────────────

  test("queryLabelDistribution groups correctly by original attack label") {
    val df = makeSyntheticDf()
    SparkSQLAnalytics.registerTempView(df, "synth_label_dist")
    val distDf = SparkSQLAnalytics.queryLabelDistribution(spark, "synth_label_dist")

    val rows = distDf.collect()
    assert(rows.length === 3) // DDoS, BENIGN, PortScan

    val counts = rows.map(r => r.getAs[String]("label_original") -> r.getAs[Long]("count")).toMap
    assert(counts("DDoS") === 3L)
    assert(counts("BENIGN") === 2L)
    assert(counts("PortScan") === 1L)

    val ddosPct = rows.find(_.getAs[String]("label_original") == "DDoS").get.getAs[Double]("percentage")
    assert(math.abs(ddosPct - 50.0) < 0.01) // 3 out of 6 = 50.0%
  }

  test("queryLabelDistribution handles empty dataset safely") {
    val df = makeEmptyDf()
    SparkSQLAnalytics.registerTempView(df, "synth_empty_label")
    val distDf = SparkSQLAnalytics.queryLabelDistribution(spark, "synth_empty_label")
    assert(distDf.count() === 0L)
  }

  // ── 5. Top Attacks Query ────────────────────────────────────

  test("queryTopAttacks filters only ATTACK labels and sorts descending") {
    val df = makeSyntheticDf()
    SparkSQLAnalytics.registerTempView(df, "synth_top_attacks")
    val topDf = SparkSQLAnalytics.queryTopAttacks(spark, limit = 5, "synth_top_attacks")

    val rows = topDf.collect()
    assert(rows.length === 2) // DDoS (3), PortScan (1); BENIGN must be excluded
    assert(rows(0).getAs[String]("label_original") === "DDoS")
    assert(rows(0).getAs[Long]("attack_count") === 3L)
    assert(rows(1).getAs[String]("label_original") === "PortScan")
    assert(rows(1).getAs[Long]("attack_count") === 1L)
  }

  // ── 6. Feature Discovery & Statistics ───────────────────────

  test("getNumericFeatureColumns excludes label columns and has_invalid_numeric") {
    val df = makeSyntheticDf()
    val numericCols = SparkSQLAnalytics.getNumericFeatureColumns(df)

    assert(numericCols.contains("destination_port"))
    assert(numericCols.contains("flow_duration"))
    assert(numericCols.contains("flow_bytes_s"))
    assert(!numericCols.contains("label_original"))
    assert(!numericCols.contains("label_binary"))
    assert(!numericCols.contains("has_invalid_numeric"))
  }

  test("queryFeatureStatistics computes correct min, max, avg, stddev") {
    val df = makeSyntheticDf()
    SparkSQLAnalytics.registerTempView(df, "synth_stats")
    val statsDf = SparkSQLAnalytics.queryFeatureStatistics(spark, Seq("flow_duration"), "synth_stats")

    val rows = statsDf.collect()
    assert(rows.length === 1)
    val r = rows.head
    assert(r.getAs[String]("feature_name") === "flow_duration")
    assert(r.getAs[Double]("min_val") === 40.0)
    assert(r.getAs[Double]("max_val") === 300.0)
    // Values: 100, 200, 50, 60, 40, 300 -> sum=750, count=6 -> avg=125.0
    assert(math.abs(r.getAs[Double]("avg_val") - 125.0) < 0.01)
  }

  // ── 7. Traffic Profile Comparison ───────────────────────────

  test("queryTrafficComparison aggregates features grouped by label_binary") {
    val df = makeSyntheticDf()
    SparkSQLAnalytics.registerTempView(df, "synth_comp")
    val compDf = SparkSQLAnalytics.queryTrafficComparison(spark, Seq("flow_duration"), "synth_comp")

    val rows = compDf.collect()
    assert(rows.length === 2)

    val resultMap = rows.map(r => r.getAs[String]("label_binary") -> (r.getAs[Long]("flow_count"), r.getAs[Double]("avg_flow_duration"))).toMap
    // Benign: 100, 200 -> avg 150.0
    assert(resultMap("BENIGN")._1 === 2L)
    assert(math.abs(resultMap("BENIGN")._2 - 150.0) < 0.01)

    // Attack: 50, 60, 40, 300 -> avg 112.5
    assert(resultMap("ATTACK")._1 === 4L)
    assert(math.abs(resultMap("ATTACK")._2 - 112.5) < 0.01)
  }

  // ── 8. Full End-to-End Pipeline & Output Validation ─────────

  test("runAnalytics executes complete suite on synthetic data") {
    val df = makeSyntheticDf()
    val res = SparkSQLAnalytics.runAnalytics(df, spark, "synth_e2e")

    assert(res.totalFlows === 6L)
    assert(res.binaryDistribution.count() === 2L)
    assert(res.labelDistribution.count() === 3L)
    assert(res.topAttacks.count() === 2L)
    assert(res.featureStats.count() === 3L)
    assert(res.trafficComparison.count() === 2L)
  }

  test("saveResults writes Parquet and CSV outputs without overwriting input dataset") {
    val tempDir = Files.createTempDirectory("sparkshield_sql_test_").toFile
    val inputDir = new File(tempDir, "input_data")
    val outputDir = new File(tempDir, "output_results")

    try {
      // 1. Write synthetic data to inputDir
      val df = makeSyntheticDf()
      df.write.parquet(inputDir.getPath)

      // 2. Load from inputDir
      val loadedDf = SparkSQLAnalytics.loadCleanedData(inputDir.getPath, spark)
      val results = SparkSQLAnalytics.runAnalytics(loadedDf, spark, "synth_io")

      // 3. Save to outputDir
      SparkSQLAnalytics.saveResults(results, outputDir.getPath, inputDir.getPath)

      // 4. Verify outputs exist
      assert(new File(outputDir, "parquet/binary_distribution/_SUCCESS").exists())
      assert(new File(outputDir, "parquet/top_attacks/_SUCCESS").exists())
      assert(new File(outputDir, "parquet/feature_stats/_SUCCESS").exists())
      assert(new File(outputDir, "csv/binary_distribution").exists())
      assert(new File(outputDir, "csv/top_attacks").exists())

      // 5. Verify source input data was NOT overwritten or corrupted
      val checkInputDf = spark.read.parquet(inputDir.getPath)
      assert(checkInputDf.count() === 6L)
      assert(checkInputDf.columns.length === 6)

      // 6. Verify error if attempting to save to inputDir
      assertThrows[IllegalArgumentException] {
        SparkSQLAnalytics.saveResults(results, inputDir.getPath, inputDir.getPath)
      }
    } finally {
      deleteRecursively(tempDir)
    }
  }

  test("loadCleanedData throws IllegalArgumentException for non-existent path") {
    assertThrows[IllegalArgumentException] {
      SparkSQLAnalytics.loadCleanedData("/non/existent/path/for/sure", spark)
    }
  }

  test("queryFeatureStatistics and queryTrafficComparison sanitize Infinity and NaN to prevent corrupted results") {
    val data = Seq(
      Row(80.0, 100.0, Double.PositiveInfinity, "BENIGN", "BENIGN", true),
      Row(80.0, 200.0, 2000.0,                  "BENIGN", "BENIGN", false),
      Row(80.0, 300.0, Double.NaN,              "DDoS",   "ATTACK", true),
      Row(80.0, 400.0, 4000.0,                  "DDoS",   "ATTACK", false)
    )
    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), testSchema)
    SparkSQLAnalytics.registerTempView(df, "synth_non_finite")

    val statsDf = SparkSQLAnalytics.queryFeatureStatistics(spark, Seq("flow_bytes_s"), "synth_non_finite")
    val statsRow = statsDf.collect().head
    assert(statsRow.getAs[String]("feature_name") === "flow_bytes_s")
    assert(statsRow.getAs[Double]("min_val") === 2000.0)
    assert(statsRow.getAs[Double]("max_val") === 4000.0)
    assert(statsRow.getAs[Double]("avg_val") === 3000.0)
    assert(!statsRow.getAs[Double]("avg_val").isInfinite)
    assert(!statsRow.getAs[Double]("avg_val").isNaN)

    val compDf = SparkSQLAnalytics.queryTrafficComparison(spark, Seq("flow_bytes_s"), "synth_non_finite")
    val compRows = compDf.collect()
    val compMap = compRows.map(r => r.getAs[String]("label_binary") -> r.getAs[Double]("avg_flow_bytes_s")).toMap

    assert(compMap("BENIGN") === 2000.0)
    assert(compMap("ATTACK") === 4000.0)
    assert(!compMap("BENIGN").isInfinite)
    assert(!compMap("ATTACK").isInfinite)
  }
}

