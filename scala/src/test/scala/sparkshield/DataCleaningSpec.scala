package sparkshield

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite

/**
 * Unit tests for DataCleaning.
 *
 * All tests use small synthetic fixtures — no CICIDS2017 data needed.
 * SparkSession is local[1] for speed.
 */
class DataCleaningSpec extends SharedSparkSession {

  override def testMaster: String = "local[1]"

  // ── Helpers ─────────────────────────────────────────────────

  /** Build a minimal DataFrame from column names and rows of strings */
  private def makeRawDf(cols: Seq[String], rows: Seq[Seq[String]]) = {
    val schema = StructType(cols.map(c => StructField(c, StringType, nullable = true)))
    val sparkRows = rows.map(r => Row(r: _*))
    spark.createDataFrame(
      spark.sparkContext.parallelize(sparkRows),
      schema
    )
  }

  // ── 1. Column name normalization ──────────────────────────

  test("normalizeColName trims leading and trailing spaces") {
    assert(DataCleaning.normalizeColName(" Destination Port ") === "destination_port")
    assert(DataCleaning.normalizeColName(" Label ") === "label")
  }

  test("normalizeColName replaces spaces and special chars with underscore") {
    assert(DataCleaning.normalizeColName("Flow Bytes/s") === "flow_bytes_s")
    assert(DataCleaning.normalizeColName("Down/Up Ratio") === "down_up_ratio")
    assert(DataCleaning.normalizeColName("Init_Win_bytes_forward") === "init_win_bytes_forward")
  }

  test("normalizeColName lowercases the result") {
    assert(DataCleaning.normalizeColName("BENIGN") === "benign")
    assert(DataCleaning.normalizeColName("Fwd IAT Total") === "fwd_iat_total")
  }

  test("normalizeColName strips leading/trailing underscores") {
    assert(DataCleaning.normalizeColName("/some/path") === "some_path")
  }

  // ── 2. Duplicate column resolution ───────────────────────

  test("buildSchema resolves duplicate 'Fwd Header Length' columns deterministically") {
    // Simulate the CICIDS2017 header with two identical col names
    val cols = Array(" Fwd Header Length", " Fwd Header Length", " Label ")
    val schema = DataCleaning.buildSchema(cols)

    assert(schema.normalizedColumns(0) === "fwd_header_length")
    assert(schema.normalizedColumns(1) === "fwd_header_length_2")
    assert(schema.duplicatesResolved.size === 1)
    assert(schema.duplicatesResolved.values.toSeq.contains("fwd_header_length_2"))
  }

  test("buildSchema assigns _2 _3 for triple duplicates") {
    val cols = Array("Alpha", "Alpha", "Alpha", " Label ")
    val schema = DataCleaning.buildSchema(cols)
    assert(schema.normalizedColumns.take(3) === Seq("alpha", "alpha_2", "alpha_3"))
  }

  test("buildSchema with no duplicates has empty duplicatesResolved map") {
    val cols = Array("ColA", "ColB", "ColC", " Label ")
    val schema = DataCleaning.buildSchema(cols)
    assert(schema.duplicatesResolved.isEmpty)
    assert(schema.labelColumn === "label")
  }

  test("buildSchema throws when no label column exists") {
    val cols = Array("ColA", "ColB")
    assertThrows[IllegalStateException] {
      DataCleaning.buildSchema(cols)
    }
  }

  // ── 3. Label normalization ────────────────────────────────

  test("label_original trims whitespace from raw label") {
    val raw = makeRawDf(
      Seq("flow_duration", " Label "),
      Seq(Seq("100", " BENIGN "), Seq("200", " DDoS "), Seq("300", " BENIGN"))
    )
    val schema = DataCleaning.buildSchema(raw.columns)
    val renamed = DataCleaning.renameColumns(raw, schema)
    val labeled = DataCleaning.normalizeLabel(renamed, schema.labelColumn)

    val labels = labeled.select("label_original").collect().map(_.getString(0))
    assert(labels.contains("BENIGN"))
    assert(labels.contains("DDoS"))
    assert(!labels.exists(_.startsWith(" ")))
    assert(!labels.exists(_.endsWith(" ")))
  }

  test("label_binary maps BENIGN->BENIGN and anything else->ATTACK") {
    val raw = makeRawDf(
      Seq("f1", " Label "),
      Seq(
        Seq("1.0", "BENIGN"),
        Seq("2.0", "DDoS"),
        Seq("3.0", "PortScan"),
        Seq("4.0", "BENIGN")
      )
    )
    val schema = DataCleaning.buildSchema(raw.columns)
    val renamed = DataCleaning.renameColumns(raw, schema)
    val labeled = DataCleaning.normalizeLabel(renamed, schema.labelColumn)

    val binaryLabels = labeled
      .select("label_original", "label_binary")
      .collect()
      .map(r => r.getString(0) -> r.getString(1))
      .toMap

    assert(binaryLabels("BENIGN") === "BENIGN")
    assert(binaryLabels("DDoS")   === "ATTACK")
    assert(binaryLabels("PortScan") === "ATTACK")
  }

  test("label_binary handles case insensitivity and maps unrecognized labels to UNKNOWN") {
    val raw = makeRawDf(
      Seq("f1", " Label "),
      Seq(
        Seq("1.0", "benign"),
        Seq("2.0", "Benign"),
        Seq("3.0", "BENIGN"),
        Seq("4.0", "  BENIGN  "),
        Seq("5.0", "ZeroDayRansomware"),
        Seq("6.0", "Infiltration-Backdoor"),
        Seq("7.0", "RandomUnrecognizedAttack"),
        Seq("8.0", null),
        Seq("9.0", ""),
        Seq("10.0", "   ")
      )
    )
    val schema = DataCleaning.buildSchema(raw.columns)
    val renamed = DataCleaning.renameColumns(raw, schema)
    val labeled = DataCleaning.normalizeLabel(renamed, schema.labelColumn)

    val rows = labeled.select("label_original", "label_binary").collect()

    // Case insensitivity & whitespace trimming: "benign", "Benign", "  BENIGN  " must all be BENIGN
    val benignRow1 = rows.find(r => Option(r.getString(0)).contains("benign")).get
    assert(benignRow1.getAs[String]("label_binary") === "BENIGN")

    val benignRow2 = rows.find(r => Option(r.getString(0)).contains("Benign")).get
    assert(benignRow2.getAs[String]("label_binary") === "BENIGN")

    val benignRow3 = rows.find(r => Option(r.getString(0)).contains("BENIGN")).get
    assert(benignRow3.getAs[String]("label_binary") === "BENIGN")

    // Unrecognized attack labels must NEVER silently map to ATTACK or BENIGN — must be UNKNOWN
    val unknownAttack1 = rows.find(r => Option(r.getString(0)).contains("ZeroDayRansomware")).get
    assert(unknownAttack1.getAs[String]("label_binary") === "UNKNOWN")

    val unknownAttack2 = rows.find(r => Option(r.getString(0)).contains("Infiltration-Backdoor")).get
    assert(unknownAttack2.getAs[String]("label_binary") === "UNKNOWN")

    val unknownAttack3 = rows.find(r => Option(r.getString(0)).contains("RandomUnrecognizedAttack")).get
    assert(unknownAttack3.getAs[String]("label_binary") === "UNKNOWN")

    // Null, empty string, or whitespace-only labels should be marked UNKNOWN
    val nullRow = rows.find(r => r.isNullAt(0) || r.getString(0) == null).get
    assert(nullRow.getAs[String]("label_binary") === "UNKNOWN")

    val emptyRow = rows.find(r => !r.isNullAt(0) && r.getString(0).isEmpty).get
    assert(emptyRow.getAs[String]("label_binary") === "UNKNOWN")
  }

  test("label_binary recognizes all explicitly supported attack classes from CICIDS2017") {
    val attacks = Seq(
      "DDoS", "ddos", "DDOS",
      "PortScan", "portscan",
      "Bot", "bot", "Botnet",
      "Infiltration",
      "FTP-Patator", "ftp-patator",
      "SSH-Patator", "ssh-patator",
      "DoS GoldenEye", "dos goldeneye",
      "DoS Hulk",
      "DoS Slowhttptest",
      "DoS slowloris",
      "Heartbleed",
      "Web Attack - Brute Force",
      "Web Attack \u0096 Brute Force",
      "Web Attack - Sql Injection",
      "Web Attack - XSS"
    )

    val rows = attacks.zipWithIndex.map { case (atk, idx) =>
      Seq(s"$idx.0", atk)
    }

    val raw = makeRawDf(Seq("f1", " Label "), rows)
    val schema = DataCleaning.buildSchema(raw.columns)
    val renamed = DataCleaning.renameColumns(raw, schema)
    val labeled = DataCleaning.normalizeLabel(renamed, schema.labelColumn)

    val collected = labeled.select("label_original", "label_binary").collect()
    assert(collected.length === attacks.length)

    collected.foreach { r =>
      val orig = r.getString(0)
      val bin = r.getString(1)
      assert(bin === "ATTACK", s"Expected $orig to map to ATTACK, but got $bin")
    }
  }

  // ── 4. Numeric casting ────────────────────────────────────

  test("castNumericColumns converts valid numeric strings to Double") {
    val raw = makeRawDf(
      Seq("feature1", "feature2", " Label "),
      Seq(Seq("1.5", "200", "BENIGN"), Seq("3.14", "0", "DDoS"))
    )
    val schema = DataCleaning.buildSchema(raw.columns)
    val renamed = DataCleaning.renameColumns(raw, schema)
    val featureCols = Seq("feature1", "feature2")
    val casted = DataCleaning.castNumericColumns(renamed, featureCols)

    val row = casted.filter(casted("feature1") === 1.5).collect().head
    assert(row.getAs[Double]("feature1") === 1.5)
    assert(row.getAs[Double]("feature2") === 200.0)
  }

  test("castNumericColumns yields null (not exception) for unparseable strings") {
    val raw = makeRawDf(
      Seq("feature1", " Label "),
      Seq(Seq("not_a_number", "BENIGN"))
    )
    val schema = DataCleaning.buildSchema(raw.columns)
    val renamed = DataCleaning.renameColumns(raw, schema)
    val casted = DataCleaning.castNumericColumns(renamed, Seq("feature1"))

    val row = casted.collect().head
    assert(row.isNullAt(row.fieldIndex("feature1")))
  }

  test("castNumericColumns handles Infinity string as Double.POSITIVE_INFINITY") {
    val raw = makeRawDf(
      Seq("flow_bytes_s", " Label "),
      Seq(Seq("Infinity", "BENIGN"), Seq("-Infinity", "BENIGN"))
    )
    val schema = DataCleaning.buildSchema(raw.columns)
    val renamed = DataCleaning.renameColumns(raw, schema)
    val casted = DataCleaning.castNumericColumns(renamed, Seq("flow_bytes_s"))

    val rows = casted.collect()
    val values = rows.map(_.getAs[Double]("flow_bytes_s"))
    assert(values.exists(v => v == Double.PositiveInfinity || v.isInfinite))
  }

  // ── 5. Invalid numeric flagging ───────────────────────────

  test("flagInvalidNumerics marks Infinity rows as true") {
    val raw = makeRawDf(
      Seq("feature1", " Label "),
      Seq(Seq("Infinity", "BENIGN"), Seq("3.14", "BENIGN"))
    )
    val schema = DataCleaning.buildSchema(raw.columns)
    val renamed = DataCleaning.renameColumns(raw, schema)
    val casted = DataCleaning.castNumericColumns(renamed, Seq("feature1"))
    val labeled = DataCleaning.normalizeLabel(casted, schema.labelColumn)
    val flagged = DataCleaning.flagInvalidNumerics(labeled, Seq("feature1"))

    val results = flagged.select("feature1", "has_invalid_numeric").collect()
    val infRow = results.find(r => r.isNullAt(0) || (r.getDouble(0).isInfinite))
    val normalRow = results.find(r => !r.isNullAt(0) && r.getDouble(0) == 3.14)

    assert(infRow.isDefined)
    assert(infRow.get.getBoolean(1) === true)
    assert(normalRow.isDefined)
    assert(normalRow.get.getBoolean(1) === false)
  }

  test("flagInvalidNumerics does NOT drop rows with invalid values") {
    val raw = makeRawDf(
      Seq("feature1", " Label "),
      Seq(Seq("Infinity", "BENIGN"), Seq("3.14", "BENIGN"), Seq("NaN", "BENIGN"))
    )
    val schema = DataCleaning.buildSchema(raw.columns)
    val renamed = DataCleaning.renameColumns(raw, schema)
    val casted = DataCleaning.castNumericColumns(renamed, Seq("feature1"))
    val labeled = DataCleaning.normalizeLabel(casted, schema.labelColumn)
    val flagged = DataCleaning.flagInvalidNumerics(labeled, Seq("feature1"))

    assert(flagged.count() === 3)  // All rows preserved
  }

  // ── 6. Null handling ─────────────────────────────────────

  test("computeNullCounts counts nulls correctly") {
    val raw = makeRawDf(
      Seq("f1", "f2", " Label "),
      Seq(Seq(null, "1.0", "BENIGN"), Seq("2.0", null, "BENIGN"), Seq("3.0", "4.0", "BENIGN"))
    )
    val schema = DataCleaning.buildSchema(raw.columns)
    val renamed = DataCleaning.renameColumns(raw, schema)
    val labeled = DataCleaning.normalizeLabel(renamed, schema.labelColumn)
    val nullCounts = DataCleaning.computeNullCounts(labeled)

    assert(nullCounts("f1") === 1)
    assert(nullCounts("f2") === 1)
  }

  // ── 7. Empty input behavior ───────────────────────────────

  test("clean handles DataFrame with header but zero data rows") {
    val raw = makeRawDf(
      Seq("f1", "f2", " Label "),
      Seq.empty  // empty
    )
    // Should not throw; all counts should be 0
    val result = DataCleaning.clean(raw)
    assert(result.rowCount === 0)
    assert(result.invalidNumerics === 0)
    assert(result.labelCounts.isEmpty)
  }

  // ── 8. Full schema consistency via clean() ─────────────────

  test("full clean() pipeline produces consistent normalized schema") {
    // Simulate a mini CICIDS2017 row with duplicate 'Fwd Header Length'
    val cols = Seq(
      " Destination Port", " Flow Duration",
      " Fwd Header Length",  // col 35 equivalent
      " Fwd Header Length",  // col 56 equivalent — duplicate
      " Label "
    )
    val rows = Seq(
      Seq("443", "1000", "40", "40", "BENIGN"),
      Seq("80",  "500",  "20", "20", "DDoS")
    )
    val raw = makeRawDf(cols, rows)
    val result = DataCleaning.clean(raw)

    val cleanCols = result.cleanDf.columns.toSeq
    assert(cleanCols.contains("destination_port"))
    assert(cleanCols.contains("fwd_header_length"))
    assert(cleanCols.contains("fwd_header_length_2"))
    assert(cleanCols.contains("label_original"))
    assert(cleanCols.contains("label_binary"))
    assert(!cleanCols.contains("label"))  // original label col removed

    assert(result.rowCount === 2)
    assert(result.labelCounts("BENIGN") === 1)
    assert(result.labelCounts("DDoS") === 1)
  }

  test("renameColumns handles the exact CICIDS2017 duplicate column scenario") {
    // Col 35 and col 56 are both " Fwd Header Length", Col 79 is "Label"
    val totalCols = (1 to 79).map {
      case 35 => " Fwd Header Length"
      case 56 => " Fwd Header Length"
      case 79 => " Label "
      case i  => s"col_$i"
    }
    val schema = DataCleaning.buildSchema(totalCols.toArray)

    assert(schema.normalizedColumns(34) === "fwd_header_length")  // col 35 (0-indexed 34)
    assert(schema.normalizedColumns(55) === "fwd_header_length_2") // col 56 (0-indexed 55)
    assert(schema.duplicatesResolved.size === 1)
  }
}
