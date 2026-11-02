package sparkshield

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite

import java.io.File
import java.nio.file.Files

/**
 * Unit tests for IntrusionDetectionModel (Milestone 5).
 *
 * Runs self-contained unit tests using synthetic DataFrames and temporary
 * directories without requiring the full 225k-row CICIDS2017 dataset.
 */
class IntrusionDetectionModelSpec extends AnyFunSuite with SharedSparkSession {

  private val testSchema = StructType(Seq(
    StructField("destination_port", DoubleType, nullable = true),
    StructField("flow_duration", DoubleType, nullable = true),
    StructField("total_fwd_packets", DoubleType, nullable = true),
    StructField("total_backward_packets", DoubleType, nullable = true),
    StructField("flow_bytes_s", DoubleType, nullable = true),
    StructField("label_original", StringType, nullable = true),
    StructField("label_binary", StringType, nullable = true),
    StructField("has_invalid_numeric", BooleanType, nullable = true)
  ))

  private def createSyntheticDf(rows: Seq[Row]): org.apache.spark.sql.DataFrame = {
    spark.createDataFrame(spark.sparkContext.parallelize(rows), testSchema)
  }

  // ── 1. Label Mapping Tests ──────────────────────────────────

  test("mapLabelToDouble maps BENIGN to 0.0 and ATTACK to 1.0 (case-insensitive)") {
    assert(IntrusionDetectionModel.mapLabelToDouble("BENIGN") === Some(0.0))
    assert(IntrusionDetectionModel.mapLabelToDouble("benign") === Some(0.0))
    assert(IntrusionDetectionModel.mapLabelToDouble("  Benign  ") === Some(0.0))
    assert(IntrusionDetectionModel.mapLabelToDouble("ATTACK") === Some(1.0))
    assert(IntrusionDetectionModel.mapLabelToDouble("attack") === Some(1.0))
    assert(IntrusionDetectionModel.mapLabelToDouble("  Attack  ") === Some(1.0))
  }

  test("mapLabelToDouble rejects null, empty, and unrecognized labels") {
    assert(IntrusionDetectionModel.mapLabelToDouble(null) === None)
    assert(IntrusionDetectionModel.mapLabelToDouble("") === None)
    assert(IntrusionDetectionModel.mapLabelToDouble("UNKNOWN") === None)
    assert(IntrusionDetectionModel.mapLabelToDouble("DDoS") === None) // DDoS must be normalized to ATTACK prior to binary labeling
  }

  test("labelToDoubleColumn accurately maps DataFrame column with nulls for invalid labels") {
    val rows = Seq(
      Row(80.0, 100.0, 2.0, 3.0, 500.0, "BENIGN", "BENIGN", false),
      Row(443.0, 200.0, 5.0, 6.0, 1000.0, "DDoS", "ATTACK", false),
      Row(22.0, 300.0, 1.0, 1.0, 100.0, "UNKNOWN", "UNKNOWN", false)
    )
    val df = createSyntheticDf(rows)
    val mapped = df.withColumn("label", IntrusionDetectionModel.labelToDoubleColumn("label_binary"))
      .select("label_binary", "label")
      .collect()

    val resultMap = mapped.map(r => r.getString(0) -> Option(r.get(1)).map(_.asInstanceOf[Double])).toMap

    assert(resultMap("BENIGN") === Some(0.0))
    assert(resultMap("ATTACK") === Some(1.0))
    assert(resultMap("UNKNOWN") === None)
  }

  // ── 2. Feature Discovery & Leakage Prevention ───────────────

  test("getFeatureColumns identifies numeric columns and excludes target and leakage fields") {
    val schema = StructType(Seq(
      StructField("destination_port", DoubleType, nullable = true),
      StructField("flow_duration", LongType, nullable = true),
      StructField("label_binary", StringType, nullable = true),
      StructField("label_original", StringType, nullable = true),
      StructField("has_invalid_numeric", BooleanType, nullable = true),
      StructField("flow_id", StringType, nullable = true),
      StructField("timestamp", StringType, nullable = true)
    ))
    val emptyDf = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
    val features = IntrusionDetectionModel.getFeatureColumns(emptyDf)

    assert(features === Seq("destination_port", "flow_duration"))
    assert(!features.contains("label_binary"))
    assert(!features.contains("label_original"))
    assert(!features.contains("has_invalid_numeric"))
    assert(!features.contains("flow_id"))
    assert(!features.contains("timestamp"))
  }

  // ── 3. Data Preparation & Non-Finite Filtering ──────────────

  test("prepareData filters out has_invalid_numeric rows and non-finite numbers") {
    val rows = Seq(
      Row(80.0, 100.0, 2.0, 3.0, 500.0, "BENIGN", "BENIGN", false),
      Row(443.0, 200.0, 5.0, 6.0, 1000.0, "DDoS", "ATTACK", false),
      // Invalid numeric flag
      Row(80.0, 300.0, 1.0, 1.0, Double.PositiveInfinity, "DDoS", "ATTACK", true),
      // NaN value in numeric feature
      Row(80.0, Double.NaN, 1.0, 1.0, 100.0, "BENIGN", "BENIGN", false),
      // Null value in numeric feature
      Row(80.0, 400.0, null, 1.0, 200.0, "BENIGN", "BENIGN", false)
    )
    val df = createSyntheticDf(rows)
    val (prepared, audit) = IntrusionDetectionModel.prepareData(df)

    assert(audit.rawRowCount === 5L)
    assert(audit.excludedRowCount === 3L)
    assert(audit.usableRowCount === 2L)
    assert(audit.usableBenignCount === 1L)
    assert(audit.usableAttackCount === 1L)
    assert(audit.featureCount === 5)
    assert(prepared.count() === 2L)
  }

  test("prepareData throws IllegalArgumentException on empty DataFrame") {
    val emptyDf = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], testSchema)
    intercept[IllegalArgumentException] {
      IntrusionDetectionModel.prepareData(emptyDf)
    }
  }

  test("prepareData throws IllegalArgumentException when label_binary is missing") {
    val schemaNoLabel = StructType(Seq(
      StructField("destination_port", DoubleType, nullable = true),
      StructField("flow_duration", DoubleType, nullable = true)
    ))
    val df = spark.createDataFrame(spark.sparkContext.parallelize(Seq(Row(80.0, 100.0))), schemaNoLabel)
    intercept[IllegalArgumentException] {
      IntrusionDetectionModel.prepareData(df)
    }
  }

  // ── 4. Split Determinism ────────────────────────────────────

  test("splitDataset produces deterministic splits with identical seed") {
    val rows = (1 to 100).map { i =>
      val label = if (i % 2 == 0) "BENIGN" else "ATTACK"
      Row(80.0, i.toDouble * 10.0, 2.0, 3.0, 500.0, label, label, false)
    }
    val df = createSyntheticDf(rows)
    val (prepared, _) = IntrusionDetectionModel.prepareData(df)

    val config1 = IntrusionDetectionModel.ModelConfig(seed = 42L, trainRatio = 0.8)
    val config2 = IntrusionDetectionModel.ModelConfig(seed = 42L, trainRatio = 0.8)

    val (_, _, summary1) = IntrusionDetectionModel.splitDataset(prepared, config1)
    val (_, _, summary2) = IntrusionDetectionModel.splitDataset(prepared, config2)

    assert(summary1.trainCount === summary2.trainCount)
    assert(summary1.testCount === summary2.testCount)
    assert(summary1.trainBenignCount === summary2.trainBenignCount)
    assert(summary1.trainAttackCount === summary2.trainAttackCount)
  }

  // ── 5. Confusion Matrix & Metric Math ───────────────────────

  test("ConfusionMatrix calculates correct accuracy, precision, recall, and F1") {
    // 80 TN, 20 FP, 5 FN, 95 TP (Total = 200)
    val cm = IntrusionDetectionModel.ConfusionMatrix(
      trueNegatives  = 80L,
      falsePositives = 20L,
      falseNegatives = 5L,
      truePositives  = 95L
    )

    assert(cm.total === 200L)
    assert(cm.accuracy === (95.0 + 80.0) / 200.0) // 0.875
    assert(cm.precision === 95.0 / (95.0 + 20.0)) // 95 / 115 ≈ 0.82608
    assert(cm.recall === 95.0 / (95.0 + 5.0))       // 95 / 100 = 0.95
    assert(cm.falsePositiveRate === 20.0 / (20.0 + 80.0)) // 20 / 100 = 0.20
    assert(cm.falseNegativeRate === 5.0 / (5.0 + 95.0))   // 5 / 100 = 0.05

    val expectedF1 = 2.0 * (cm.precision * cm.recall) / (cm.precision + cm.recall)
    assert(math.abs(cm.f1Score - expectedF1) < 1e-6)
  }

  test("ConfusionMatrix handles zero division edge cases safely") {
    val emptyCm = IntrusionDetectionModel.ConfusionMatrix(0L, 0L, 0L, 0L)
    assert(emptyCm.accuracy === 0.0)
    assert(emptyCm.precision === 0.0)
    assert(emptyCm.recall === 0.0)
    assert(emptyCm.f1Score === 0.0)
    assert(emptyCm.falsePositiveRate === 0.0)
    assert(emptyCm.falseNegativeRate === 0.0)
  }

  // ── 6. Pipeline End-to-End Execution on Synthetic Data ──────

  test("ML Pipeline trains, evaluates, and produces valid predictions and artifacts") {
    // Generate 60 synthetic records with clear separable patterns:
    // BENIGN: low port (80/443), low duration (100.0), low packets (2.0)
    // ATTACK: high port (50000), high duration (10000.0), high packets (200.0)
    val benignRows = (1 to 30).map { i =>
      Row(80.0 + (i % 2), 100.0 + i, 2.0, 2.0, 500.0, "BENIGN", "BENIGN", false)
    }
    val attackRows = (1 to 30).map { i =>
      Row(50000.0 + (i % 2), 10000.0 + i, 200.0, 150.0, 50000.0, "DDoS", "ATTACK", false)
    }

    val df = createSyntheticDf(benignRows ++ attackRows)
    val (prepared, audit) = IntrusionDetectionModel.prepareData(df)

    val tempDir = Files.createTempDirectory("sparkshield-ml-test-").toFile
    try {
      val config = IntrusionDetectionModel.ModelConfig(
        outputBaseDir = tempDir.getAbsolutePath,
        trainRatio    = 0.70,
        seed          = 42L,
        maxIter       = 10,
        regParam      = 0.01
      )

      val (trainDf, testDf, splitSummary) = IntrusionDetectionModel.splitDataset(prepared, config)
      val pipeline = IntrusionDetectionModel.buildPipeline(audit.featureNames, config)
      val model = pipeline.fit(trainDf)
      val predictions = model.transform(testDf)

      val metrics = IntrusionDetectionModel.evaluate(predictions)

      // Separable synthetic dataset should achieve high metrics
      assert(metrics.confusionMatrix.accuracy >= 0.90)
      assert(metrics.rocAuc >= 0.90)

      val runDir = s"${config.outputBaseDir}/test-run"
      IntrusionDetectionModel.saveArtifacts(
        model,
        predictions,
        metrics,
        audit,
        splitSummary,
        config,
        runDir
      )

      assert(new File(s"$runDir/model").exists())
      assert(new File(s"$runDir/metrics.json").exists())
      assert(new File(s"$runDir/features.json").exists())
      assert(new File(s"$runDir/evaluation-summary.txt").exists())
      assert(new File(s"$runDir/predictions_sample.csv").exists())

      // Verify latest_run.txt pointer file was generated
      val latestFile = new File(s"${config.outputBaseDir}/latest_run.txt")
      assert(latestFile.exists(), "latest_run.txt pointer file should be written by saveArtifacts")
      val pointerContent = scala.io.Source.fromFile(latestFile).getLines().next().trim
      assert(pointerContent === runDir)
    } finally {
      deleteRecursively(tempDir)
    }
  }

  // ── 7. Confusion Matrix & Metric Calculation Edge Cases ─────

  test("ConfusionMatrix correctly calculates metrics under extreme class imbalance and boundary conditions") {
    // Perfect detector
    val perfect = IntrusionDetectionModel.ConfusionMatrix(
      trueNegatives = 100, falsePositives = 0, falseNegatives = 0, truePositives = 100
    )
    assert(perfect.total === 200)
    assert(perfect.accuracy === 1.0)
    assert(perfect.precision === 1.0)
    assert(perfect.recall === 1.0)
    assert(perfect.f1Score === 1.0)
    assert(perfect.falsePositiveRate === 0.0)
    assert(perfect.falseNegativeRate === 0.0)

    // Complete miss (zero TP, zero FP - all predicted benign)
    val completeMiss = IntrusionDetectionModel.ConfusionMatrix(
      trueNegatives = 100, falsePositives = 0, falseNegatives = 50, truePositives = 0
    )
    assert(completeMiss.total === 150)
    assert(completeMiss.accuracy === 100.0 / 150.0)
    assert(completeMiss.precision === 0.0) // Safe division by zero
    assert(completeMiss.recall === 0.0)
    assert(completeMiss.f1Score === 0.0)
    assert(completeMiss.falsePositiveRate === 0.0)
    assert(completeMiss.falseNegativeRate === 1.0)

    // Complete false alarm (zero TN, zero FN - all predicted attack)
    val allAlarm = IntrusionDetectionModel.ConfusionMatrix(
      trueNegatives = 0, falsePositives = 50, falseNegatives = 0, truePositives = 100
    )
    assert(allAlarm.total === 150)
    assert(allAlarm.accuracy === 100.0 / 150.0)
    assert(allAlarm.precision === 100.0 / 150.0)
    assert(allAlarm.recall === 1.0)
    assert(allAlarm.falsePositiveRate === 1.0)
    assert(allAlarm.falseNegativeRate === 0.0)

    // Empty matrix (0 total) does not trigger NaN / divide by zero
    val emptyCm = IntrusionDetectionModel.ConfusionMatrix(0, 0, 0, 0)
    assert(emptyCm.total === 0)
    assert(emptyCm.accuracy === 0.0)
    assert(emptyCm.precision === 0.0)
    assert(emptyCm.recall === 0.0)
    assert(emptyCm.f1Score === 0.0)
    assert(emptyCm.falsePositiveRate === 0.0)
    assert(emptyCm.falseNegativeRate === 0.0)
  }

  test("parseArgs parses CLI arguments including --test-input and hyperparameter overrides") {
    val args = Array(
      "--input", "data/custom/train",
      "--test-input", "data/custom/test",
      "--output-dir", "results/custom-ml",
      "--train-ratio", "0.75",
      "--seed", "123",
      "--max-iter", "30",
      "--reg-param", "0.05",
      "--threshold", "0.6"
    )
    val config = IntrusionDetectionModel.parseArgs(args)

    assert(config.inputPath === "data/custom/train")
    assert(config.testInputPath === Some("data/custom/test"))
    assert(config.outputBaseDir === "results/custom-ml")
    assert(config.trainRatio === 0.75)
    assert(config.seed === 123L)
    assert(config.maxIter === 30)
    assert(config.regParam === 0.05)
    assert(config.threshold === 0.6)
  }
}
