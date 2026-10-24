package sparkshield

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.NumericType
import org.slf4j.LoggerFactory

import java.io.File
import java.time.Instant

// ============================================================
// SparkSQLAnalytics — Milestone 3
//
// Performs analytical querying on cleaned CICIDS2017 network flow
// data using Apache Spark SQL and temporary views.
//
// Key capabilities:
//   1. Validates required schema fields upfront with helpful errors.
//   2. Registers DataFrame as temporary SQL view: `network_flows`.
//   3. Runs Spark SQL aggregations for:
//      - Total flow volume
//      - Binary label distribution (counts & percentages, zero-safe)
//      - Multi-class attack distribution (by label_original)
//      - Top attacks sorted by count
//      - Descriptive statistics (min, max, avg, stddev) for numeric features
//      - Benign vs Attack behavioral traffic profile comparisons
//   4. Excludes label fields and flags from numeric feature discovery.
//   5. Saves results to configurable output paths (Parquet & CSV)
//      without overwriting source data.
// ============================================================
object SparkSQLAnalytics {

  private val log = LoggerFactory.getLogger(getClass)

  val DefaultViewName: String = "network_flows"
  val RequiredColumns: Set[String] = Set("label_binary", "label_original", "has_invalid_numeric")
  val ExcludedFeatureColumns: Set[String] = Set("label_original", "label_binary", "label", "has_invalid_numeric")

  case class AnalyticsResults(
    totalFlows: Long,
    binaryDistribution: DataFrame,
    labelDistribution: DataFrame,
    topAttacks: DataFrame,
    featureStats: DataFrame,
    trafficComparison: DataFrame
  )

  // ── 1. Schema Validation ───────────────────────────────────

  /**
   * Validate that the DataFrame contains all required columns.
   * Throws IllegalArgumentException if any mandatory columns are missing.
   *
   * @param df       Input DataFrame
   * @param required Set of column names that must exist
   */
  def validateSchema(df: DataFrame, required: Set[String] = RequiredColumns): Unit = {
    val existingCols = df.columns.toSet
    val missing = required.diff(existingCols)
    if (missing.nonEmpty) {
      throw new IllegalArgumentException(
        s"Dataset is missing required columns: [${missing.toList.sorted.mkString(", ")}]. " +
        s"Available columns (${existingCols.size}): [${df.columns.sorted.mkString(", ")}]"
      )
    }
  }

  // ── 2. Data Loading & View Registration ────────────────────

  /**
   * Load cleaned Parquet dataset into a DataFrame and validate columns.
   *
   * @param path  Path to cleaned Parquet directory
   * @param spark Active SparkSession
   * @return Loaded DataFrame
   */
  def loadCleanedData(path: String, spark: SparkSession): DataFrame = {
    val f = new File(path)
    if (!f.exists()) {
      throw new IllegalArgumentException(s"Input Parquet path does not exist: $path")
    }
    log.info(s"Loading cleaned Parquet data from: $path")
    val df = spark.read.parquet(path)
    validateSchema(df)
    df
  }

  /**
   * Register DataFrame as temporary SQL view.
   *
   * @param df       Input DataFrame
   * @param viewName Name of the SQL view (default: "network_flows")
   */
  def registerTempView(df: DataFrame, viewName: String = DefaultViewName): Unit = {
    validateSchema(df)
    df.createOrReplaceTempView(viewName)
    log.info(s"Registered temporary view '$viewName' with ${df.columns.length} columns")
  }

  // ── 3. Analytical Queries via Spark SQL ─────────────────────

  /**
   * Query total flow count via Spark SQL.
   */
  def queryTotalFlows(spark: SparkSession, viewName: String = DefaultViewName): Long = {
    val sql = s"SELECT count(*) AS total_flows FROM $viewName"
    spark.sql(sql).collect().head.getAs[Long]("total_flows")
  }

  /**
   * Query benign vs attack distribution (counts and percentages) using label_binary.
   * Handles empty input safely: returns 0.0 percentage without division by zero.
   */
  def queryBinaryDistribution(spark: SparkSession, viewName: String = DefaultViewName): DataFrame = {
    val total = queryTotalFlows(spark, viewName)
    if (total == 0L) {
      val sql =
        s"""
           |SELECT
           |  label_binary,
           |  count(*) AS count,
           |  CAST(0.0 AS DOUBLE) AS percentage
           |FROM $viewName
           |GROUP BY label_binary
           |ORDER BY count DESC
         """.stripMargin
      spark.sql(sql)
    } else {
      val sql =
        s"""
           |SELECT
           |  label_binary,
           |  count(*) AS count,
           |  CAST(round((count(*) * 100.0) / $total, 4) AS DOUBLE) AS percentage
           |FROM $viewName
           |GROUP BY label_binary
           |ORDER BY count DESC
         """.stripMargin
      spark.sql(sql)
    }
  }

  /**
   * Query attack distribution grouped by label_original.
   * Handles empty input safely: returns 0.0 percentage without division by zero.
   */
  def queryLabelDistribution(spark: SparkSession, viewName: String = DefaultViewName): DataFrame = {
    val total = queryTotalFlows(spark, viewName)
    if (total == 0L) {
      val sql =
        s"""
           |SELECT
           |  label_original,
           |  label_binary,
           |  count(*) AS count,
           |  CAST(0.0 AS DOUBLE) AS percentage
           |FROM $viewName
           |GROUP BY label_original, label_binary
           |ORDER BY count DESC
         """.stripMargin
      spark.sql(sql)
    } else {
      val sql =
        s"""
           |SELECT
           |  label_original,
           |  label_binary,
           |  count(*) AS count,
           |  CAST(round((count(*) * 100.0) / $total, 4) AS DOUBLE) AS percentage
           |FROM $viewName
           |GROUP BY label_original, label_binary
           |ORDER BY count DESC
         """.stripMargin
      spark.sql(sql)
    }
  }

  /**
   * Query top attack labels sorted by count descending (excluding BENIGN).
   */
  def queryTopAttacks(spark: SparkSession, limit: Int = 10, viewName: String = DefaultViewName): DataFrame = {
    val sql =
      s"""
         |SELECT
         |  label_original,
         |  count(*) AS attack_count
         |FROM $viewName
         |WHERE upper(label_binary) = 'ATTACK'
         |GROUP BY label_original
         |ORDER BY attack_count DESC
         |LIMIT $limit
       """.stripMargin
    spark.sql(sql)
  }

  /**
   * Discover numeric traffic feature columns, strictly excluding label fields and flags.
   */
  def getNumericFeatureColumns(df: DataFrame): Seq[String] = {
    df.schema.fields
      .filter(f => f.dataType.isInstanceOf[NumericType])
      .map(_.name)
      .filterNot(ExcludedFeatureColumns.contains)
  }

  /**
   * Generates a Spark SQL expression that sanitizes a numeric column for statistical aggregations.
   * If non-finite (NaN, +/-Infinity) or null, returns NULL so Spark SQL aggregation functions
   * (min, max, avg, stddev) safely ignore it without corrupting results to Infinity or NaN.
   * Does NOT replace invalid values with arbitrary numbers.
   */
  def finiteSqlExpr(colName: String): String =
    s"(CASE WHEN `$colName` IS NOT NULL AND NOT isnan(`$colName`) AND abs(`$colName`) < CAST('Infinity' AS DOUBLE) THEN `$colName` ELSE NULL END)"

  /**
   * Compute descriptive statistics (min, max, avg, stddev) for numeric features via Spark SQL.
   * Computes statistics across all features in a single distributed pass.
   *
   * @param spark       Active SparkSession
   * @param featureCols Optional subset of feature columns (if empty, auto-detects all numeric features)
   * @param viewName    SQL temporary view name
   * @return DataFrame with columns: [feature_name, min_val, max_val, avg_val, stddev_val]
   */
  def queryFeatureStatistics(
    spark: SparkSession,
    featureCols: Seq[String] = Nil,
    viewName: String = DefaultViewName
  ): DataFrame = {
    val df = spark.table(viewName)
    val colsToAnalyze = if (featureCols.nonEmpty) {
      featureCols.filterNot(ExcludedFeatureColumns.contains)
    } else {
      getNumericFeatureColumns(df)
    }

    if (colsToAnalyze.isEmpty) {
      import spark.implicits._
      return Seq.empty[(String, Double, Double, Double, Double)]
        .toDF("feature_name", "min_val", "max_val", "avg_val", "stddev_val")
    }

    // Build a single aggregated SQL expression to avoid scanning the dataset multiple times
    val aggExprs = colsToAnalyze.flatMap { c =>
      val f = finiteSqlExpr(c)
      Seq(
        s"min($f) AS `min_$c`",
        s"max($f) AS `max_$c`",
        s"round(avg($f), 4) AS `avg_$c`",
        s"round(stddev($f), 4) AS `stddev_$c`"
      )
    }

    val query = s"SELECT ${aggExprs.mkString(", ")} FROM $viewName"
    val summaryRow = spark.sql(query).head()

    val records = colsToAnalyze.map { c =>
      val minVal = if (summaryRow.isNullAt(summaryRow.fieldIndex(s"min_$c"))) Double.NaN else summaryRow.getAs[Double](s"min_$c")
      val maxVal = if (summaryRow.isNullAt(summaryRow.fieldIndex(s"max_$c"))) Double.NaN else summaryRow.getAs[Double](s"max_$c")
      val avgVal = if (summaryRow.isNullAt(summaryRow.fieldIndex(s"avg_$c"))) Double.NaN else summaryRow.getAs[Double](s"avg_$c")
      val stdVal = if (summaryRow.isNullAt(summaryRow.fieldIndex(s"stddev_$c"))) Double.NaN else summaryRow.getAs[Double](s"stddev_$c")
      (c, minVal, maxVal, avgVal, stdVal)
    }

    import spark.implicits._
    records.toDF("feature_name", "min_val", "max_val", "avg_val", "stddev_val")
  }

  /**
   * Compare traffic behavior statistics between BENIGN and ATTACK traffic profiles.
   */
  def queryTrafficComparison(
    spark: SparkSession,
    trafficCols: Seq[String] = Nil,
    viewName: String = DefaultViewName
  ): DataFrame = {
    val df = spark.table(viewName)
    val availableCols = df.columns.toSet
    val defaultCols = Seq("flow_duration", "flow_bytes_s", "flow_packets_s", "total_fwd_packets", "total_backward_packets")
    val selectedCols = (if (trafficCols.nonEmpty) trafficCols else defaultCols).filter(availableCols.contains)

    if (selectedCols.isEmpty) {
      val sql =
        s"""
           |SELECT
           |  label_binary,
           |  count(*) AS flow_count
           |FROM $viewName
           |GROUP BY label_binary
           |ORDER BY flow_count DESC
         """.stripMargin
      spark.sql(sql)
    } else {
      val aggExprs = selectedCols.flatMap { col =>
        val f = finiteSqlExpr(col)
        Seq(
          s"round(avg($f), 4) AS `avg_$col`",
          s"min($f) AS `min_$col`",
          s"max($f) AS `max_$col`"
        )
      }.mkString(",\n  ")

      val sql =
        s"""
           |SELECT
           |  label_binary,
           |  count(*) AS flow_count,
           |  $aggExprs
           |FROM $viewName
           |GROUP BY label_binary
           |ORDER BY flow_count DESC
         """.stripMargin
      spark.sql(sql)
    }
  }

  // ── 4. Pipeline Execution ──────────────────────────────────

  /**
   * Execute full analytical query suite on DataFrame.
   */
  def runAnalytics(
    df: DataFrame,
    spark: SparkSession,
    viewName: String = DefaultViewName
  ): AnalyticsResults = {
    validateSchema(df)
    registerTempView(df, viewName)

    val totalFlows = queryTotalFlows(spark, viewName)
    val binaryDist = queryBinaryDistribution(spark, viewName)
    val labelDist = queryLabelDistribution(spark, viewName)
    val topAttacks = queryTopAttacks(spark, limit = 10, viewName)
    val featureStats = queryFeatureStatistics(spark, viewName = viewName)
    val trafficComp = queryTrafficComparison(spark, viewName = viewName)

    AnalyticsResults(
      totalFlows = totalFlows,
      binaryDistribution = binaryDist,
      labelDistribution = labelDist,
      topAttacks = topAttacks,
      featureStats = featureStats,
      trafficComparison = trafficComp
    )
  }

  // ── 5. Output Management ───────────────────────────────────

  /**
   * Save analytical query results to Parquet and coalesced CSV formats.
   * Strictly prevents overwriting the source input dataset.
   */
  def saveResults(
    results: AnalyticsResults,
    outputDir: String,
    inputPath: String = ""
  ): Unit = {
    val outDirFile = new File(outputDir).getCanonicalFile
    if (inputPath.nonEmpty) {
      val inDirFile = new File(inputPath).getCanonicalFile
      if (outDirFile.equals(inDirFile) || outDirFile.toPath.startsWith(inDirFile.toPath)) {
        throw new IllegalArgumentException(
          s"Output path '$outputDir' collides with input path '$inputPath'. Overwriting input data is forbidden."
        )
      }
    }

    outDirFile.mkdirs()

    // 1. Columnar Parquet format (for downstream analytics)
    val parquetDir = new File(outDirFile, "parquet")
    results.binaryDistribution.write.mode("overwrite").parquet(new File(parquetDir, "binary_distribution").getPath)
    results.labelDistribution.write.mode("overwrite").parquet(new File(parquetDir, "label_distribution").getPath)
    results.topAttacks.write.mode("overwrite").parquet(new File(parquetDir, "top_attacks").getPath)
    results.featureStats.write.mode("overwrite").parquet(new File(parquetDir, "feature_stats").getPath)
    results.trafficComparison.write.mode("overwrite").parquet(new File(parquetDir, "traffic_comparison").getPath)

    // 2. Coalesced CSV format (1 partition per table for human-readable reporting)
    // Note on Spark CSV writes: Distributed DataFrames naturally write partitioned CSV files
    // (part-*.csv). Coalescing to 1 partition ensures a single clean CSV file per table.
    val csvDir = new File(outDirFile, "csv")
    results.binaryDistribution.coalesce(1).write.option("header", "true").mode("overwrite").csv(new File(csvDir, "binary_distribution").getPath)
    results.labelDistribution.coalesce(1).write.option("header", "true").mode("overwrite").csv(new File(csvDir, "label_distribution").getPath)
    results.topAttacks.coalesce(1).write.option("header", "true").mode("overwrite").csv(new File(csvDir, "top_attacks").getPath)
    results.trafficComparison.coalesce(1).write.option("header", "true").mode("overwrite").csv(new File(csvDir, "traffic_comparison").getPath)
    results.featureStats.coalesce(1).write.option("header", "true").mode("overwrite").csv(new File(csvDir, "feature_stats").getPath)

    log.info(s"Saved analytics outputs to $outputDir (Parquet and coalesced CSV)")
  }

  /**
   * Save a lightweight JSON summary report of the key metrics.
   */
  def saveSummaryJson(
    results: AnalyticsResults,
    inputPath: String,
    outputDir: String
  ): Unit = {
    import java.io.PrintWriter

    val reportDir = new File("results/metrics")
    reportDir.mkdirs()
    val reportFile = new File(reportDir, "sql-analytics-summary.json")

    val binaryMap = results.binaryDistribution.collect().map { r =>
      s"""    "${r.getAs[String]("label_binary")}": { "count": ${r.getAs[Long]("count")}, "percentage": ${r.getAs[Double]("percentage")} }"""
    }.mkString(",\n")

    val json =
      s"""{
         |  "generated_at": "${Instant.now()}",
         |  "input_dataset": "$inputPath",
         |  "output_directory": "$outputDir",
         |  "total_flows": ${results.totalFlows},
         |  "binary_distribution": {
         |$binaryMap
         |  },
         |  "top_attacks_count": ${results.topAttacks.count()},
         |  "analyzed_features_count": ${results.featureStats.count()}
         |}""".stripMargin

    val pw = new PrintWriter(reportFile)
    try { pw.println(json) } finally { pw.close() }
    log.info(s"Summary metrics saved to: ${reportFile.getAbsolutePath}")
  }

  // ── 6. CLI Entry Point ─────────────────────────────────────

  def main(args: Array[String]): Unit = {
    log.info("SparkShield — SparkSQLAnalytics starting")

    val spark = SparkSessionFactory.create(
      "SparkShield-SparkSQLAnalytics",
      extras = Map("spark.sql.shuffle.partitions" -> "8")
    )

    val defaultInput = "data/processed/cleaned_2026-10-24"
    val defaultOutput = "results/sql-analytics"

    val inputPath = if (args.nonEmpty) args(0) else defaultInput
    val outputDir = if (args.length > 1) args(1) else defaultOutput

    println("\n" + "=" * 75)
    println("  SparkShield — Milestone 3: Spark SQL Analytics")
    println(s"  Input Parquet : $inputPath")
    println(s"  Output Dir    : $outputDir")
    println("=" * 75)

    // Load and run analytics
    val df = loadCleanedData(inputPath, spark)
    val results = runAnalytics(df, spark)

    println(s"\n── Total Analyzed Flows: ${results.totalFlows} ─────────────────────────────")

    println("\n── 1. Binary Label Distribution (BENIGN vs ATTACK) ───────────────────")
    results.binaryDistribution.show(truncate = false)

    println("\n── 2. Attack Distribution by Original Label ──────────────────────────")
    results.labelDistribution.show(truncate = false)

    println("\n── 3. Top Attack Labels (Sorted by Count Descending) ─────────────────")
    results.topAttacks.show(truncate = false)

    println("\n── 4. Key Traffic Feature Descriptive Statistics (Sample) ────────────")
    results.featureStats.show(15, truncate = false)

    println("\n── 5. Benign vs Attack Traffic Comparison ────────────────────────────")
    results.trafficComparison.show(truncate = false)

    // Save outputs
    saveResults(results, outputDir, inputPath)
    saveSummaryJson(results, inputPath, outputDir)

    println("\n" + "=" * 75)
    println(s"  Analytics successfully saved to: $outputDir")
    println("  Summary report saved to: results/metrics/sql-analytics-summary.json")
    println("  SparkSQLAnalytics — COMPLETE")
    println("=" * 75 + "\n")

    spark.stop()
    log.info("SparkShield — SparkSQLAnalytics finished")
  }
}
