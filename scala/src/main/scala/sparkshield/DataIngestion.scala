package sparkshield

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

import java.io.File

// ============================================================
// DataIngestion — Milestone 2
//
// Reads one or many CICIDS2017 CSV files using Spark's
// DataFrame CSV reader (distributed — never loads everything
// into driver memory).
//
// Usage (spark-submit):
//   sparkshield.DataIngestion <path>  [single csv or directory]
//   sparkshield.DataIngestion --file <path>
//   sparkshield.DataIngestion --dir  <path>
//   sparkshield.DataIngestion --sample <path>  (reads first file only)
//
// Config fallback: SPARKSHIELD_DATASET_DIR env var or
//   -Dsparkshield.dataset.dir=<path> system property.
// ============================================================
object DataIngestion {

  private val log = LoggerFactory.getLogger(getClass)

  // ── Public API (reused by other modules) ──────────────────

  /**
   * Read a single CSV file into a DataFrame.
   *
   * Spark reads the CSV in a distributed, lazy fashion.
   * No data is collected to the driver.
   *
   * @param path  Absolute path to a single CSV file
   * @param spark SparkSession
   * @return Raw DataFrame (all columns as String initially)
   */
  def readSingleFile(path: String, spark: SparkSession): DataFrame = {
    validatePath(path, isDir = false)
    log.info(s"Reading single CSV: $path")
    spark.read
      .option("header", "true")
      .option("inferSchema", "false")  // we'll cast explicitly in DataCleaning
      .option("mode", "PERMISSIVE")    // don't fail on malformed rows
      .option("quote", "\"")
      .option("escape", "\"")
      .csv(path)
  }

  /**
   * Read ALL CICIDS2017 CSV files in a directory.
   *
   * Spark reads them as one logical DataFrame; partitioning is
   * distributed across files.
   *
   * @param dir   Directory containing CICIDS2017 CSV files
   * @param spark SparkSession
   * @return Raw DataFrame (all columns as String initially)
   */
  def readAllFiles(dir: String, spark: SparkSession): DataFrame = {
    validatePath(dir, isDir = true)
    val csvGlob = s"$dir/*.csv"
    log.info(s"Reading all CSVs from directory: $dir")
    spark.read
      .option("header", "true")
      .option("inferSchema", "false")
      .option("mode", "PERMISSIVE")
      .option("quote", "\"")
      .option("escape", "\"")
      .csv(csvGlob)
  }

  /**
   * Read a sample (N rows) from a single CSV.
   *
   * Uses Spark's fraction-based sample so the full file is
   * never collected to the driver.
   *
   * @param path     CSV file path
   * @param fraction Sampling fraction 0.0–1.0
   * @param seed     Random seed for reproducibility
   * @param spark    SparkSession
   */
  def readSample(
    path    : String,
    fraction: Double = 0.01,
    seed    : Long   = 42L,
    spark   : SparkSession
  ): DataFrame = {
    validatePath(path, isDir = false)
    log.info(s"Reading sample (fraction=$fraction) from: $path")
    readSingleFile(path, spark).sample(withReplacement = false, fraction, seed)
  }

  /**
   * Print schema and basic statistics without collecting
   * the full dataset to the driver.
   */
  def inspect(df: DataFrame, label: String = "DataFrame"): Unit = {
    log.info(s"Inspecting $label")
    println(s"\n── Schema: $label ─────────────────────────────────────")
    println(s"  Columns : ${df.columns.length}")
    df.printSchema()
    println(s"  Partitions (approx): ${df.rdd.getNumPartitions}")
  }

  // ── CLI entry point ───────────────────────────────────────

  def main(args: Array[String]): Unit = {
    log.info("SparkShield — DataIngestion starting")

    val spark = SparkSessionFactory.create("SparkShield-DataIngestion")

    val (mode, path) = parseArgs(args)

    println("\n" + "=" * 70)
    println("  SparkShield — Data Ingestion")
    println("=" * 70)

    val df = mode match {
      case "dir"    =>
        println(s"  Mode  : read all CSVs in directory")
        println(s"  Path  : $path")
        readAllFiles(path, spark)

      case "sample" =>
        println(s"  Mode  : sample (1%) from file")
        println(s"  Path  : $path")
        readSample(path, fraction = 0.01, spark = spark)

      case _ =>  // "file" (default)
        println(s"  Mode  : read single file")
        println(s"  Path  : $path")
        readSingleFile(path, spark)
    }

    inspect(df, s"raw ($mode)")

    // Show a bounded preview — never collect the whole dataset
    println(s"\n── First 3 rows (raw, truncated) ───────────────────────────────────")
    df.show(3, truncate = 60)

    // Count via distributed action (no collect)
    println(s"── Row count ────────────────────────────────────────────────────────")
    val count = df.count()
    println(s"  Total rows : $count")

    // Label column inspection (distributed groupBy)
    val labelCol = findLabelColumn(df)
    labelCol match {
      case Some(col) =>
        println(s"\n── Label distribution (column: '$col') ─────────────────────────────")
        df.groupBy(col).count().orderBy(org.apache.spark.sql.functions.desc("count")).show(30, truncate = false)
      case None =>
        println(s"\n  [WARN] No 'Label' column found. Columns: ${df.columns.mkString(", ")}")
    }

    println("\n" + "=" * 70)
    println("  DataIngestion — COMPLETE")
    println("=" * 70 + "\n")

    spark.stop()
    log.info("SparkShield — DataIngestion finished")
  }

  // ── Helpers ───────────────────────────────────────────────

  /** Find the Label column regardless of leading/trailing spaces */
  def findLabelColumn(df: DataFrame): Option[String] =
    df.columns.find(_.trim.equalsIgnoreCase("label"))

  /** Validate a path exists and is the expected type */
  private def validatePath(path: String, isDir: Boolean): Unit = {
    val f = new File(path)
    if (!f.exists()) {
      throw new IllegalArgumentException(
        s"Path does not exist: $path\n" +
        s"  Set SPARKSHIELD_DATASET_DIR or pass the path as an argument."
      )
    }
    if (isDir && !f.isDirectory) {
      throw new IllegalArgumentException(
        s"Expected a directory but got a file: $path"
      )
    }
    if (!isDir && !f.isFile) {
      throw new IllegalArgumentException(
        s"Expected a file but got a directory: $path"
      )
    }
  }

  /**
   * Resolves default dataset directory from system properties, environment variable,
   * or standard Downloads fallback.
   */
  def defaultDatasetDir: String =
    Option(System.getProperty("sparkshield.dataset.dir"))
      .orElse(Option(System.getenv("SPARKSHIELD_DATASET_DIR")))
      .getOrElse(System.getProperty("user.home") + "/Downloads/MachineLearningCVE")

  /** Parse CLI args into (mode, path) */
  private def parseArgs(args: Array[String]): (String, String) = {
    val defaultDir  = defaultDatasetDir
    val defaultFile = s"$defaultDir/Monday-WorkingHours.pcap_ISCX.csv"

    if (args.isEmpty) {
      log.info(s"No args provided — defaulting to single file: $defaultFile")
      return ("file", defaultFile)
    }

    args.toList match {
      case "--dir"    :: path :: _ => ("dir",    path)
      case "--file"   :: path :: _ => ("file",   path)
      case "--sample" :: path :: _ => ("sample", path)
      case path :: _ if path.endsWith(".csv") => ("file", path)
      case path :: _ => ("dir", path)
    }
  }
}
