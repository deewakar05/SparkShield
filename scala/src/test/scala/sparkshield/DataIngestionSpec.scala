package sparkshield

import org.scalatest.funsuite.AnyFunSuite

import java.io.{File, PrintWriter}
import java.nio.file.Files

/**
 * Unit tests for DataIngestion (Milestone 2).
 *
 * Uses temporary CSV files and directories with synthetic data.
 * Does not require the full CICIDS2017 dataset.
 */
class DataIngestionSpec extends SharedSparkSession {

  override def testMaster: String = "local[1]"

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

  // ── 1. Path Validation & Inaccessible Paths ─────────────────

  test("readSingleFile throws IllegalArgumentException for non-existent path") {
    assertThrows[IllegalArgumentException] {
      DataIngestion.readSingleFile("/tmp/non_existent_file_sparkshield.csv", spark)
    }
  }

  test("readSingleFile throws IllegalArgumentException if given a directory instead of a file") {
    val tempDir = Files.createTempDirectory("sparkshield_ingest_dir_").toFile
    try {
      val ex = intercept[IllegalArgumentException] {
        DataIngestion.readSingleFile(tempDir.getAbsolutePath, spark)
      }
      assert(ex.getMessage.contains("Expected a file but got a directory"))
    } finally {
      deleteRecursively(tempDir)
    }
  }

  test("readAllFiles throws IllegalArgumentException if given a file instead of a directory") {
    val tempFile = File.createTempFile("sparkshield_test_", ".csv")
    try {
      val ex = intercept[IllegalArgumentException] {
        DataIngestion.readAllFiles(tempFile.getAbsolutePath, spark)
      }
      assert(ex.getMessage.contains("Expected a directory but got a file"))
    } finally {
      tempFile.delete()
    }
  }

  // ── 2. Valid CSV Input & Schema Preservation ────────────────

  test("readSingleFile correctly loads columns and rows as String DataFrame") {
    val tempFile = File.createTempFile("valid_flow_", ".csv")
    try {
      writeCsv(
        tempFile,
        " Destination Port, Flow Duration, Label ",
        Seq(
          "80, 1500, BENIGN",
          "443, 3200, DDoS"
        )
      )

      val df = DataIngestion.readSingleFile(tempFile.getAbsolutePath, spark)
      assert(df.count() === 2L)
      assert(df.columns.length === 3)
      assert(df.columns.contains(" Destination Port"))
      assert(df.columns.contains(" Label "))

      val collected = df.collect()
      assert(collected.length === 2)
      assert(collected(0).getString(0).trim === "80")
      assert(collected(1).getString(2).trim === "DDoS")
    } finally {
      tempFile.delete()
    }
  }

  // ── 3. Quoted Fields and Delimiters ─────────────────────────

  test("readSingleFile parses quoted fields containing commas correctly") {
    val tempFile = File.createTempFile("quoted_flow_", ".csv")
    try {
      writeCsv(
        tempFile,
        "flow_id,notes,Label",
        Seq(
          "1,\"Normal, verified traffic\",BENIGN",
          "2,\"Attack: SYN flood, port 80\",DDoS"
        )
      )

      val df = DataIngestion.readSingleFile(tempFile.getAbsolutePath, spark)
      assert(df.count() === 2L)
      val rows = df.collect()
      assert(rows(0).getAs[String]("notes") === "Normal, verified traffic")
      assert(rows(1).getAs[String]("notes") === "Attack: SYN flood, port 80")
    } finally {
      tempFile.delete()
    }
  }

  // ── 4. Empty Files and Permissive Parsing ───────────────────

  test("readSingleFile reads CSV with header but zero data rows") {
    val tempFile = File.createTempFile("empty_data_", ".csv")
    try {
      writeCsv(tempFile, "Destination Port,Flow Duration,Label", Seq.empty)
      val df = DataIngestion.readSingleFile(tempFile.getAbsolutePath, spark)
      assert(df.count() === 0L)
      assert(df.columns.length === 3)
    } finally {
      tempFile.delete()
    }
  }

  test("readSingleFile handles malformed rows in PERMISSIVE mode without crashing") {
    val tempFile = File.createTempFile("malformed_rows_", ".csv")
    try {
      writeCsv(
        tempFile,
        "colA,colB,Label",
        Seq(
          "10,20,BENIGN",
          "only_one_col",           // missing fields
          "1,2,3,extra_fourth_col"   // extra field
        )
      )

      val df = DataIngestion.readSingleFile(tempFile.getAbsolutePath, spark)
      // Permissive mode should read all 3 rows without throwing an exception
      assert(df.count() === 3L)
    } finally {
      tempFile.delete()
    }
  }

  // ── 5. Directory Ingestion (Multiple CSVs) ───────────────────

  test("readAllFiles merges all CSVs in a directory into a single DataFrame") {
    val tempDir = Files.createTempDirectory("multi_csv_dir_").toFile
    try {
      val f1 = new File(tempDir, "part1.csv")
      val f2 = new File(tempDir, "part2.csv")

      writeCsv(f1, "Destination Port,Label", Seq("80,BENIGN", "22,PortScan"))
      writeCsv(f2, "Destination Port,Label", Seq("443,BENIGN", "8080,DDoS"))

      val df = DataIngestion.readAllFiles(tempDir.getAbsolutePath, spark)
      assert(df.count() === 4L)
      assert(df.columns.toSeq === Seq("Destination Port", "Label"))
    } finally {
      deleteRecursively(tempDir)
    }
  }

  // ── 6. Deterministic Sampling ───────────────────────────────

  test("readSample produces deterministic sample with seed") {
    val tempFile = File.createTempFile("sample_test_", ".csv")
    try {
      val rows = (1 to 100).map(i => s"$i,BENIGN")
      writeCsv(tempFile, "id,Label", rows)

      val s1 = DataIngestion.readSample(tempFile.getAbsolutePath, fraction = 0.2, seed = 42L, spark = spark).collect()
      val s2 = DataIngestion.readSample(tempFile.getAbsolutePath, fraction = 0.2, seed = 42L, spark = spark).collect()

      assert(s1.length > 0)
      assert(s1.length < 100)
      // With the same seed, the sampled records must match exactly
      assert(s1.map(_.getString(0)) === s2.map(_.getString(0)))
    } finally {
      tempFile.delete()
    }
  }

  // ── 7. Label Column Discovery ───────────────────────────────

  test("findLabelColumn finds label column case-insensitively with leading/trailing spaces") {
    val tempFile = File.createTempFile("find_label_", ".csv")
    try {
      writeCsv(tempFile, "fwd_pkts,  LABEL  ,bwd_pkts", Seq("1,BENIGN,2"))
      val df = DataIngestion.readSingleFile(tempFile.getAbsolutePath, spark)

      val found = DataIngestion.findLabelColumn(df)
      assert(found.isDefined)
      assert(found.get === "  LABEL  ")
    } finally {
      tempFile.delete()
    }
  }

  test("findLabelColumn returns None when no column matches label") {
    val tempFile = File.createTempFile("no_label_", ".csv")
    try {
      writeCsv(tempFile, "fwd_pkts,duration,bwd_pkts", Seq("1,100,2"))
      val df = DataIngestion.readSingleFile(tempFile.getAbsolutePath, spark)

      val found = DataIngestion.findLabelColumn(df)
      assert(found.isEmpty)
    } finally {
      tempFile.delete()
    }
  }
}
