package sparkshield

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, LongType}
import org.slf4j.LoggerFactory

// ============================================================
// DataCleaning — Milestone 2
//
// Implements a reproducible, documented cleaning pipeline for
// the CICIDS2017 network-flow CSV dataset.
//
// Cleaning decisions (all explicit — nothing silent):
//
// C1 — Column name normalization:
//      Trim leading/trailing whitespace, lowercase, replace
//      all non-alphanumeric runs with a single underscore.
//
// C2 — Duplicate column resolution:
//      Column 35 (0-indexed 34): `fwd_header_length`   → kept
//      Column 56 (0-indexed 55): `fwd_header_length`   → renamed
//        to `fwd_header_length_2` (position-based, deterministic).
//      All collisions resolved with `_N` suffix in order of
//      first occurrence.
//
// C3 — Label normalization:
//      Trim whitespace from the `label` column.
//      Derive `label_binary`: BENIGN → "BENIGN", else → "ATTACK".
//      Preserve original label as `label_original`.
//
// C4 — Numeric casting:
//      78 feature columns cast from String → Double.
//      The string "Infinity" → Double.PositiveInfinity (Spark
//      handles this natively when casting from String).
//      Rows with NaN or infinite values are flagged in a
//      separate boolean column `has_invalid_numeric`.
//      They are NOT dropped — decision left to ML pipeline.
//
// C5 — Null handling:
//      Nulls from PERMISSIVE CSV parsing are preserved and
//      counted. Not silently zero-filled.
//
// C6 — Label column discovery:
//      Uses case-insensitive, whitespace-trimmed search.
//      Fails explicitly if no label column is found.
// ============================================================
object DataCleaning {

  private val log = LoggerFactory.getLogger(getClass)

  // ── Column name normalization constants ───────────────────

  /**
   * The original header at column index 34 (1-indexed: 35)
   * and 55 (1-indexed: 56) are both " Fwd Header Length".
   * After normalization both become "fwd_header_length".
   * We resolve the second occurrence to "fwd_header_length_2".
   *
   * This map is the authoritative original → normalized mapping.
   * Built lazily from the actual DataFrame schema.
   */
  case class CleanSchema(
    originalToNormalized : Map[String, String],  // original name → normalized name
    normalizedColumns    : Seq[String],           // final ordered column list
    duplicatesResolved   : Map[String, String],   // original dup → renamed
    labelColumn          : String,                // name of the label column (after normalizing)
    rawColumnCount       : Int = 0                // count of original raw columns
  )

  // ── Result container ──────────────────────────────────────

  case class CleaningResult(
    cleanDf    : DataFrame,
    schema     : CleanSchema,
    rowCount   : Long,
    nullCounts  : Map[String, Long],   // col → null count
    invalidNumerics: Long,             // rows with any Infinity/NaN in numeric cols
    labelCounts: Map[String, Long]     // label value → count
  )

  // ── Main public API ───────────────────────────────────────

  /**
   * Full cleaning pipeline.
   *
   * @param rawDf   Raw DataFrame from DataIngestion (all String columns)
   * @return        CleaningResult with annotated clean DataFrame
   */
  def clean(rawDf: DataFrame): CleaningResult = {
    log.info("DataCleaning: starting pipeline")

    // Step 1: Build normalized schema
    val schema = buildSchema(rawDf.columns)
    logSchema(schema)

    // Step 2: Rename columns
    val renamedDf = renameColumns(rawDf, schema)

    // Step 3: Identify feature columns (everything except label cols)
    val labelNorm = schema.labelColumn  // e.g., "label"
    val featureCols = schema.normalizedColumns.filterNot { c =>
      c == labelNorm || c == "label_original" || c == "label_binary"
    }

    // Step 4: Cast feature columns to Double
    val castedDf = castNumericColumns(renamedDf, featureCols)

    // Step 5: Normalize label column
    val labeledDf = normalizeLabel(castedDf, labelNorm)

    // Step 6: Flag rows with any Infinity or NaN in numeric columns
    val flaggedDf = flagInvalidNumerics(labeledDf, featureCols)

    // Step 7: Cache for multiple aggregations
    flaggedDf.cache()

    // Step 8: Compute quality metrics (distributed — no collect on full data)
    log.info("DataCleaning: computing row count (distributed)")
    val rowCount = flaggedDf.count()

    log.info("DataCleaning: computing null counts per column")
    val nullCounts = computeNullCounts(flaggedDf)

    log.info("DataCleaning: computing invalid numeric count")
    val invalidNumerics = flaggedDf
      .filter(col("has_invalid_numeric") === true)
      .count()

    log.info("DataCleaning: computing label distribution")
    val labelCounts = flaggedDf
      .groupBy("label_original")
      .count()
      .collect()  // small: at most ~15 distinct labels
      .map(r => r.getString(0) -> r.getLong(1))
      .toMap

    flaggedDf.unpersist()

    log.info(s"DataCleaning: complete. rows=$rowCount, invalidNumerics=$invalidNumerics")

    CleaningResult(
      cleanDf        = flaggedDf,
      schema         = schema,
      rowCount       = rowCount,
      nullCounts     = nullCounts,
      invalidNumerics= invalidNumerics,
      labelCounts    = labelCounts
    )
  }

  // ── Schema building ───────────────────────────────────────

  /**
   * Build the normalized schema from the raw column array.
   *
   * Normalization rules (C1, C2):
   *   1. Trim each name
   *   2. Lowercase
   *   3. Replace any sequence of non-alphanumeric chars with "_"
   *   4. Strip leading/trailing underscores
   *   5. On collision, append "_2", "_3", ... (position order)
   */
  def buildSchema(rawColumns: Array[String]): CleanSchema = {
    val seenCounts = scala.collection.mutable.Map[String, Int]()
    val originalToNorm = scala.collection.mutable.LinkedHashMap[String, String]()
    val duplicatesResolved = scala.collection.mutable.Map[String, String]()
    val normalizedCols = scala.collection.mutable.ArrayBuffer[String]()

    rawColumns.zipWithIndex.foreach { case (original, idx) =>
      val rawBase = if (original.endsWith(idx.toString) && normalizeColName(original.dropRight(idx.toString.length)).nonEmpty) {
        val candidate = original.dropRight(idx.toString.length)
        val isSparkSafeHeader = rawColumns.zipWithIndex.exists { case (other, otherIdx) =>
          other != original && (
            other == candidate ||
            (other.endsWith(otherIdx.toString) && other.dropRight(otherIdx.toString.length) == candidate)
          )
        }
        if (isSparkSafeHeader) candidate else original
      } else original

      val base = normalizeColName(rawBase)
      val count = seenCounts.getOrElse(base, 0)
      val finalName = if (count == 0) base else s"${base}_${count + 1}"
      if (count > 0) duplicatesResolved(rawBase.trim) = finalName
      seenCounts(base) = count + 1
      if (!originalToNorm.contains(original)) {
        originalToNorm(original) = finalName
      }
      normalizedCols += finalName
    }

    val normalizedColumns = normalizedCols.toSeq
    val labelColumn = normalizedColumns
      .find(_.equalsIgnoreCase("label"))
      .getOrElse {
        throw new IllegalStateException(
          "No 'label' column found in dataset after normalization.\n" +
          s"Available columns: ${normalizedColumns.mkString(", ")}"
        )
      }

    CleanSchema(
      originalToNormalized = originalToNorm.toMap,
      normalizedColumns    = normalizedColumns,
      duplicatesResolved   = duplicatesResolved.toMap,
      labelColumn          = labelColumn,
      rawColumnCount       = rawColumns.length
    )
  }

  /** Normalize a single column name (rules C1) */
  def normalizeColName(raw: String): String = {
    raw.trim
      .toLowerCase
      .replaceAll("[^a-z0-9]+", "_")
      .stripPrefix("_")
      .stripSuffix("_")
  }

  // ── Rename step ───────────────────────────────────────────

  /**
   * Rename all raw columns to their normalized names.
   * Uses positional renaming (toDF) to safely handle duplicate original names.
   */
  def renameColumns(df: DataFrame, schema: CleanSchema): DataFrame = {
    df.toDF(schema.normalizedColumns: _*)
  }

  // ── Numeric casting ───────────────────────────────────────

  /**
   * Cast each feature column from String to Double.
   *
   * Spark's cast from String → Double handles:
   *   - Normal numerics → Double value
   *   - "Infinity"      → Double.POSITIVE_INFINITY
   *   - "-Infinity"     → Double.NEGATIVE_INFINITY
   *   - Unparseable     → null (documented, not silent zero)
   *
   * We do NOT use toDouble on the driver; all casting is
   * distributed inside Spark executor JVMs.
   */
  def castNumericColumns(df: DataFrame, featureCols: Seq[String]): DataFrame = {
    featureCols.foldLeft(df) { (accDf, colName) =>
      accDf.withColumn(colName, col(colName).cast(DoubleType))
    }
  }

  // ── Supported Ground-Truth Attack Classes ─────────────────
  /**
   * Ground-truth attack labels derived from the CICIDS2017 dataset
   * and project specifications.
   */
  val SupportedAttackLabels: Set[String] = Set(
    "DDOS",
    "PORTSCAN",
    "BOT",
    "BOTNET",
    "INFILTRATION",
    "FTP-PATATOR",
    "SSH-PATATOR",
    "DOS GOLDENEYE",
    "DOS HULK",
    "DOS SLOWHTTPTEST",
    "DOS SLOWLORIS",
    "HEARTBLEED",
    "WEB ATTACK - BRUTE FORCE",
    "WEB ATTACK - SQL INJECTION",
    "WEB ATTACK - XSS",
    "WEB ATTACK \u0096 BRUTE FORCE",
    "WEB ATTACK \u0096 SQL INJECTION",
    "WEB ATTACK \u0096 XSS",
    "WEB ATTACK \uFFFD BRUTE FORCE",
    "WEB ATTACK \uFFFD SQL INJECTION",
    "WEB ATTACK \uFFFD XSS"
  )

  /**
   * Evaluates whether a column contains an explicitly supported attack class.
   * Handles case insensitivity, whitespace, hyphens, and non-breaking dashes.
   */
  def isRecognizedAttack(c: Column): Column = {
    val cleanUpper = upper(trim(c))
    val regexCleaned = upper(trim(regexp_replace(c, "[^a-zA-Z0-9]+", " ")))
    cleanUpper.isin(SupportedAttackLabels.toSeq: _*) ||
      regexCleaned.isin(
        "DDOS", "PORTSCAN", "BOT", "BOTNET", "INFILTRATION",
        "FTP PATATOR", "SSH PATATOR", "DOS GOLDENEYE", "DOS HULK",
        "DOS SLOWHTTPTEST", "DOS SLOWLORIS", "HEARTBLEED",
        "WEB ATTACK BRUTE FORCE", "WEB ATTACK SQL INJECTION", "WEB ATTACK XSS"
      )
  }

  // ── Label normalization ───────────────────────────────────

  /**
   * Normalize the label column (C3 & Release Gate Label Integrity):
   *   - Trim whitespace from the raw label value
   *   - Preserve trimmed original in `label_original`
   *   - Case-insensitively map BENIGN → "BENIGN"
   *   - Case-insensitively map recognized attack classes → "ATTACK"
   *   - Map null, empty, whitespace-only, and unrecognized labels → "UNKNOWN"
   *   - Never silently map unrecognized labels to ATTACK or BENIGN
   *   - Drop the old raw label column (replaced by label_original)
   */
  def normalizeLabel(df: DataFrame, labelColName: String): DataFrame = {
    val trimmedLabel = trim(col(labelColName))
    val isBenign = upper(trimmedLabel) === "BENIGN"
    val isAttack = isRecognizedAttack(trimmedLabel)

    df
      .withColumn("label_original", trimmedLabel)
      .withColumn(
        "label_binary",
        when(trimmedLabel.isNull || length(trimmedLabel) === 0, lit("UNKNOWN"))
          .when(isBenign, lit("BENIGN"))
          .when(isAttack, lit("ATTACK"))
          .otherwise(lit("UNKNOWN"))
      )
      .drop(labelColName)
  }

  // ── Invalid numeric flagging ──────────────────────────────

  /**
   * Add a boolean column `has_invalid_numeric` = true when
   * any feature column contains Infinity or NaN (C4).
   *
   * Rows flagged here are preserved in the cleaned DataFrame.
   * The ML pipeline decides whether to drop or impute them.
   */
  /**
   * Identifies if a numeric column value is non-finite (NaN, +Infinity, -Infinity).
   */
  def isNonFinite(c: Column): Column =
    c.isNotNull && (c.isNaN || c === Double.PositiveInfinity || c === Double.NegativeInfinity)

  def flagInvalidNumerics(df: DataFrame, featureCols: Seq[String]): DataFrame = {
    // Build an OR condition across all numeric feature columns
    val invalidCondition = featureCols.map(c => isNonFinite(col(c))).reduce(_ || _)

    df.withColumn("has_invalid_numeric", invalidCondition)
  }

  // ── Null counting ─────────────────────────────────────────

  /**
   * Count nulls per column using a single distributed pass.
   * Uses Spark's built-in `count` / `summing isnull` aggregation.
   * Avoids multiple scans by computing all columns in one action.
   */
  def computeNullCounts(df: DataFrame): Map[String, Long] = {
    val nullExprs = df.columns.map(c => sum(col(c).isNull.cast(LongType)).alias(c))
    val row = df.agg(nullExprs.head, nullExprs.tail: _*).collect().head
    df.columns.zipWithIndex.map { case (colName, idx) =>
      colName -> (if (row.isNullAt(idx)) 0L else row.getLong(idx))
    }.toMap
  }

  // ── Reporting ─────────────────────────────────────────────

  def printReport(result: CleaningResult, sourceFile: String): Unit = {
    val r = result
    println("\n" + "=" * 70)
    println("  SparkShield — Data Quality Report")
    println("=" * 70)

    println(s"\n── Source ───────────────────────────────────────────────────────────")
    println(s"  File     : $sourceFile")
    println(s"  Raw cols : ${r.schema.originalToNormalized.size}")
    println(s"  Clean cols: ${r.schema.normalizedColumns.size + 2}") // +label_original, +label_binary

    println(s"\n── Duplicate Column Resolution (C2) ─────────────────────────────────")
    if (r.schema.duplicatesResolved.isEmpty) {
      println("  No duplicates resolved.")
    } else {
      r.schema.duplicatesResolved.foreach { case (orig, renamed) =>
        println(s"  Original: '$orig' → renamed to '$renamed'")
      }
    }

    println(s"\n── Row Counts ───────────────────────────────────────────────────────")
    println(s"  Total rows        : ${r.rowCount}")
    println(s"  Rows with Infinity/NaN in any numeric col: ${r.invalidNumerics}")
    println(s"  Invalid % of total: ${"%.2f".format(100.0 * r.invalidNumerics / r.rowCount)}%")

    println(s"\n── Label Distribution (label_original) ──────────────────────────────")
    r.labelCounts.toSeq.sortBy(-_._2).foreach { case (lbl, cnt) =>
      val pct = "%.2f".format(100.0 * cnt / r.rowCount)
      println(f"  ${lbl}%-40s $cnt%8d  ($pct%%)")
    }

    println(s"\n── Columns with any Nulls ───────────────────────────────────────────")
    val withNulls = r.nullCounts.filter(_._2 > 0).toSeq.sortBy(-_._2)
    if (withNulls.isEmpty) {
      println("  No null values found in any column.")
    } else {
      withNulls.foreach { case (c, n) => println(f"  ${c}%-45s $n%8d nulls") }
    }

    println("\n" + "=" * 70)
  }

  private def logSchema(s: CleanSchema): Unit = {
    log.info(s"Schema built: ${s.normalizedColumns.size} normalized columns")
    log.info(s"Label column: ${s.labelColumn}")
    if (s.duplicatesResolved.nonEmpty) {
      s.duplicatesResolved.foreach { case (orig, renamed) =>
        log.warn(s"Duplicate column renamed: '$orig' -> '$renamed'")
      }
    }
  }

  // ── CLI entry point ───────────────────────────────────────

  def main(args: Array[String]): Unit = {
    log.info("SparkShield — DataCleaning starting")

    val spark = SparkSessionFactory.create(
      "SparkShield-DataCleaning",
      extras = Map("spark.sql.shuffle.partitions" -> "8")
    )

    val defaultDir = DataIngestion.defaultDatasetDir

    val inputPath = if (args.nonEmpty) args(0)
      else s"$defaultDir/Friday-WorkingHours-Afternoon-DDos.pcap_ISCX.csv"

    val outputPath = if (args.length > 1) args(1)
      else s"data/processed/cleaned_${java.time.LocalDate.now()}"

    println("\n" + "=" * 70)
    println("  SparkShield — Data Cleaning Pipeline")
    println(s"  Input  : $inputPath")
    println(s"  Output : $outputPath")
    println("=" * 70)

    // Read
    val rawDf = DataIngestion.readSingleFile(inputPath, spark)
    println(s"\n  Raw schema: ${rawDf.columns.length} columns")

    // Clean
    val result = clean(rawDf)

    // Report
    printReport(result, inputPath)

    // Show clean schema
    println("\n── Clean DataFrame schema ───────────────────────────────────────────")
    result.cleanDf.printSchema()

    // Show sample (bounded, no collect of full data)
    println("\n── Sample (5 rows, key columns) ─────────────────────────────────────")
    result.cleanDf.select(
      "destination_port", "flow_duration", "flow_bytes_s",
      "has_invalid_numeric", "label_original", "label_binary"
    ).show(5, truncate = false)

    // Write output
    println(s"\n── Writing Parquet to: $outputPath ──────────────────────────────────")

    val outputDir = new java.io.File(outputPath)
    if (outputDir.exists()) {
      println(s"  [WARN] Output path already exists: $outputPath")
      println(s"  Skipping write to avoid overwriting. Delete the directory first if you want to re-run.")
    } else {
      result.cleanDf.write
        .mode("error")    // fail if exists — explicit, not silent overwrite
        .parquet(outputPath)
      println(s"  Written successfully.")
    }

    // Save JSON quality report
    saveJsonReport(result, inputPath, outputPath)

    println("\n" + "=" * 70)
    println("  DataCleaning — COMPLETE")
    println("=" * 70 + "\n")

    spark.stop()
    log.info("SparkShield — DataCleaning finished")
  }

  // ── JSON report writer ────────────────────────────────────

  def saveJsonReport(result: CleaningResult, inputPath: String, outputPath: String): Unit = {
    import java.io.{File, PrintWriter}
    import java.time.Instant

    val reportDir = new File("results/metrics")
    reportDir.mkdirs()

    val reportFile = new File(reportDir, "data-quality-report.json")

    val nullsJson = result.nullCounts
      .filter(_._2 > 0)
      .toSeq.sortBy(-_._2)
      .map { case (c, n) => s"""    "$c": $n""" }
      .mkString(",\n")

    val labelsJson = result.labelCounts.toSeq.sortBy(-_._2)
      .map { case (l, c) => s"""    ${jsonStr(l)}: $c""" }
      .mkString(",\n")

    val dupsJson = result.schema.duplicatesResolved
      .map { case (o, r) => s"""    ${jsonStr(o)}: ${jsonStr(r)}""" }
      .mkString(",\n")

    val json =
      s"""{
         |  "generated_at": "${Instant.now()}",
         |  "input_file": ${jsonStr(inputPath)},
         |  "output_path": ${jsonStr(outputPath)},
         |  "raw_column_count": ${result.schema.rawColumnCount},
         |  "clean_column_count": ${result.cleanDf.columns.length},
         |  "row_count": ${result.rowCount},
         |  "rows_with_invalid_numerics": ${result.invalidNumerics},
         |  "invalid_numeric_pct": ${"%.4f".format(100.0 * result.invalidNumerics / result.rowCount)},
         |  "duplicate_columns_resolved": {
         |$dupsJson
         |  },
         |  "label_distribution": {
         |$labelsJson
         |  },
         |  "null_counts_nonzero": {
         |$nullsJson
         |  },
         |  "cleaning_rules": {
         |    "C1_col_normalization": "trim, lowercase, replace non-alphanumeric with underscore",
         |    "C2_duplicate_resolution": "second occurrence of same normalized name gets _2 suffix",
         |    "C3_label_normalization": "trim whitespace; derive label_binary (BENIGN vs ATTACK)",
         |    "C4_numeric_casting": "String->Double; Infinity preserved as Double.POSITIVE_INFINITY; cast failure -> null",
         |    "C5_invalid_flagging": "has_invalid_numeric=true for rows with Infinity or NaN; rows NOT dropped",
         |    "C6_null_handling": "nulls from PERMISSIVE parsing preserved and counted; not zero-filled"
         |  }
         |}""".stripMargin

    val pw = new PrintWriter(reportFile)
    try { pw.println(json) } finally { pw.close() }

    println(s"\n  Data quality report saved: ${reportFile.getAbsolutePath}")
  }

  private def jsonStr(s: String): String = s""""${s.replace("\\", "\\\\").replace("\"", "\\\"")}""""
}
