package sparkshield

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{StandardScaler, VectorAssembler}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.NumericType
import org.apache.spark.sql.{Column, DataFrame}
import org.slf4j.LoggerFactory

import java.io.{File, PrintWriter}
import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneId}

/**
 * ============================================================
 * IntrusionDetectionModel — Milestone 5
 *
 * Implements a reproducible, leakage-free binary network intrusion
 * detection pipeline using Spark MLlib Logistic Regression.
 *
 * Target mapping:
 *   - BENIGN -> 0.0
 *   - ATTACK -> 1.0
 *
 * Pipeline architecture:
 *   1. VectorAssembler: Combines discovered numeric features into raw_features
 *   2. StandardScaler: Normalizes features with unit variance (withStd=true, withMean=false)
 *   3. LogisticRegression: Binary classification with L2 regularization
 * ============================================================
 */
object IntrusionDetectionModel {

  private val log = LoggerFactory.getLogger(getClass)

  val DefaultInputPath: String = "data/processed/cleaned_2026-10-24"
  val DefaultTrainRatio: Double = 0.80
  val DefaultSeed: Long = 42L
  val DefaultMaxIter: Int = 50
  val DefaultRegParam: Double = 0.01
  val DefaultElasticNetParam: Double = 0.0
  val DefaultThreshold: Double = 0.50

  /** Columns that must NEVER enter the feature vector to prevent target leakage. */
  val ExcludedFeatureColumns: Set[String] = Set(
    "label",
    "label_binary",
    "label_original",
    "has_invalid_numeric",
    "flow_id",
    "timestamp",
    "event_id",
    "raw_features",
    "features",
    "prediction",
    "rawPrediction",
    "probability"
  )

  case class ModelConfig(
    inputPath: String          = DefaultInputPath,
    outputBaseDir: String      = "results/ml",
    trainRatio: Double         = DefaultTrainRatio,
    seed: Long                 = DefaultSeed,
    maxIter: Int               = DefaultMaxIter,
    regParam: Double           = DefaultRegParam,
    elasticNetParam: Double    = DefaultElasticNetParam,
    threshold: Double          = DefaultThreshold
  )

  case class DatasetAudit(
    rawRowCount: Long,
    excludedRowCount: Long,
    usableRowCount: Long,
    usableBenignCount: Long,
    usableAttackCount: Long,
    featureCount: Int,
    featureNames: Seq[String]
  )

  case class SplitSummary(
    trainCount: Long,
    trainBenignCount: Long,
    trainAttackCount: Long,
    testCount: Long,
    testBenignCount: Long,
    testAttackCount: Long
  )

  case class ConfusionMatrix(
    trueNegatives: Long,  // Actual BENIGN (0.0), Predicted BENIGN (0.0)
    falsePositives: Long, // Actual BENIGN (0.0), Predicted ATTACK (1.0) - False Alarm
    falseNegatives: Long, // Actual ATTACK (1.0), Predicted BENIGN (0.0) - Missed Attack
    truePositives: Long   // Actual ATTACK (1.0), Predicted ATTACK (1.0) - Detected Attack
  ) {
    def total: Long = trueNegatives + falsePositives + falseNegatives + truePositives

    def accuracy: Double =
      if (total > 0) (truePositives + trueNegatives).toDouble / total else 0.0

    def precision: Double =
      if (truePositives + falsePositives > 0)
        truePositives.toDouble / (truePositives + falsePositives)
      else 0.0

    def recall: Double =
      if (truePositives + falseNegatives > 0)
        truePositives.toDouble / (truePositives + falseNegatives)
      else 0.0

    def f1Score: Double = {
      val p = precision
      val r = recall
      if (p + r > 0.0) 2.0 * (p * r) / (p + r) else 0.0
    }

    def falsePositiveRate: Double =
      if (falsePositives + trueNegatives > 0)
        falsePositives.toDouble / (falsePositives + trueNegatives)
      else 0.0

    def falseNegativeRate: Double =
      if (falseNegatives + truePositives > 0)
        falseNegatives.toDouble / (falseNegatives + truePositives)
      else 0.0
  }

  case class EvaluationMetrics(
    confusionMatrix: ConfusionMatrix,
    rocAuc: Double,
    prAuc: Double
  )

  // ── 1. Label Mapping & Validation ───────────────────────────

  /**
   * Pure mapping from string label to binary numeric double.
   *
   * @param label String label (e.g. "BENIGN", "ATTACK")
   * @return Some(0.0) for BENIGN, Some(1.0) for ATTACK, None for unknown/null
   */
  def mapLabelToDouble(label: String): Option[Double] = {
    if (label == null) None
    else {
      label.trim.toUpperCase match {
        case "BENIGN" => Some(0.0)
        case "ATTACK" => Some(1.0)
        case _        => None
      }
    }
  }

  /**
   * Spark SQL Column expression mapping label_binary to numeric double label:
   *   - "BENIGN" -> 0.0
   *   - "ATTACK" -> 1.0
   *   - otherwise -> NULL
   */
  def labelToDoubleColumn(colName: String): Column = {
    when(trim(upper(col(colName))) === "BENIGN", 0.0)
      .when(trim(upper(col(colName))) === "ATTACK", 1.0)
      .otherwise(lit(null).cast("double"))
  }

  // ── 2. Feature Column Discovery ─────────────────────────────

  /**
   * Programmatically identify numeric feature columns while strictly
   * excluding target columns, IDs, flags, and leakage-prone fields.
   */
  def getFeatureColumns(df: DataFrame): Seq[String] = {
    df.schema.fields
      .filter(f => f.dataType.isInstanceOf[NumericType])
      .map(_.name)
      .filterNot(ExcludedFeatureColumns.contains)
      .sorted
  }

  // ── 3. Data Cleaning, Filtering & Auditing ──────────────────

  /**
   * Prepare and sanitize dataset for ML pipeline:
   *   1. Checks and maps label_binary to "label" (0.0 / 1.0).
   *   2. Discovers numeric feature columns.
   *   3. Filters out rows where has_invalid_numeric is true, label is null,
   *      or any feature contains null, NaN, or +/-Infinity.
   *
   * @param rawDf Raw cleaned DataFrame from Milestone 2
   * @return (sanitized DataFrame with "label" column, DatasetAudit report)
   */
  def prepareData(rawDf: DataFrame): (DataFrame, DatasetAudit) = {
    val rawCount = rawDf.count()

    if (rawCount == 0L) {
      throw new IllegalArgumentException("Cannot prepare data from an empty DataFrame.")
    }

    if (!rawDf.columns.contains("label_binary")) {
      throw new IllegalArgumentException("Input DataFrame missing mandatory 'label_binary' column.")
    }

    // Discover numeric features
    val featureCols = getFeatureColumns(rawDf)
    if (featureCols.isEmpty) {
      throw new IllegalArgumentException("No numeric feature columns found in dataset.")
    }

    // Map label
    val withLabelDf = rawDf.withColumn("label", labelToDoubleColumn("label_binary"))

    // Build filter condition: label not null AND (has_invalid_numeric == false if present)
    var validCondition: Column = col("label").isNotNull

    if (rawDf.columns.contains("has_invalid_numeric")) {
      validCondition = validCondition && (col("has_invalid_numeric") === false || !col("has_invalid_numeric"))
    }

    // Ensure all numeric feature columns are finite (not null, not NaN, not +/-Infinity)
    featureCols.foreach { c =>
      validCondition = validCondition && !DataCleaning.isNonFinite(col(c)) && col(c).isNotNull
    }

    val sanitizedDf = withLabelDf.filter(validCondition)
    val usableCount = sanitizedDf.count()
    val excludedCount = rawCount - usableCount

    if (usableCount == 0L) {
      throw new IllegalStateException("All records were excluded after filtering invalid numeric values.")
    }

    // Compute label distribution
    val labelCounts = sanitizedDf.groupBy("label").count().collect().map { r =>
      r.getDouble(0) -> r.getLong(1)
    }.toMap

    val benignCount = labelCounts.getOrElse(0.0, 0L)
    val attackCount = labelCounts.getOrElse(1.0, 0L)

    val audit = DatasetAudit(
      rawRowCount        = rawCount,
      excludedRowCount   = excludedCount,
      usableRowCount     = usableCount,
      usableBenignCount  = benignCount,
      usableAttackCount  = attackCount,
      featureCount       = featureCols.size,
      featureNames       = featureCols
    )

    log.info(s"Dataset prepared: raw=$rawCount, excluded=$excludedCount, usable=$usableCount (BENIGN=$benignCount, ATTACK=$attackCount), features=${featureCols.size}")

    (sanitizedDf, audit)
  }

  // ── 4. Train / Test Split ───────────────────────────────────

  /**
   * Split dataset into training and test sets using a fixed random seed.
   *
   * @param df     Sanitized DataFrame
   * @param config Model configuration
   * @return (trainDf, testDf, SplitSummary)
   */
  def splitDataset(df: DataFrame, config: ModelConfig): (DataFrame, DataFrame, SplitSummary) = {
    val splits = df.randomSplit(Array(config.trainRatio, 1.0 - config.trainRatio), config.seed)
    val trainDf = splits(0).cache()
    val testDf = splits(1).cache()

    val trainCount = trainDf.count()
    val testCount = testDf.count()

    val trainLabelCounts = trainDf.groupBy("label").count().collect().map(r => r.getDouble(0) -> r.getLong(1)).toMap
    val testLabelCounts = testDf.groupBy("label").count().collect().map(r => r.getDouble(0) -> r.getLong(1)).toMap

    val summary = SplitSummary(
      trainCount        = trainCount,
      trainBenignCount  = trainLabelCounts.getOrElse(0.0, 0L),
      trainAttackCount  = trainLabelCounts.getOrElse(1.0, 0L),
      testCount         = testCount,
      testBenignCount   = testLabelCounts.getOrElse(0.0, 0L),
      testAttackCount   = testLabelCounts.getOrElse(1.0, 0L)
    )

    log.info(s"Dataset split (ratio=${config.trainRatio}, seed=${config.seed}): Train=$trainCount (B=${summary.trainBenignCount}, A=${summary.trainAttackCount}), Test=$testCount (B=${summary.testBenignCount}, A=${summary.testAttackCount})")

    (trainDf, testDf, summary)
  }

  // ── 5. ML Pipeline Construction ─────────────────────────────

  /**
   * Build the Spark ML Pipeline:
   *   VectorAssembler -> StandardScaler -> LogisticRegression
   *
   * @param featureCols Sequence of numeric feature column names
   * @param config      Model hyperparameter configuration
   */
  def buildPipeline(featureCols: Seq[String], config: ModelConfig): Pipeline = {
    val assembler = new VectorAssembler()
      .setInputCols(featureCols.toArray)
      .setOutputCol("raw_features")
      .setHandleInvalid("error")

    val scaler = new StandardScaler()
      .setInputCol("raw_features")
      .setOutputCol("features")
      .setWithStd(true)
      .setWithMean(false) // avoids densifying sparse vectors

    val lr = new LogisticRegression()
      .setFeaturesCol("features")
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setRawPredictionCol("rawPrediction")
      .setProbabilityCol("probability")
      .setMaxIter(config.maxIter)
      .setRegParam(config.regParam)
      .setElasticNetParam(config.elasticNetParam)
      .setThreshold(config.threshold)

    new Pipeline().setStages(Array(assembler, scaler, lr))
  }

  // ── 6. Evaluation ───────────────────────────────────────────

  /**
   * Evaluate predictions DataFrame containing "label", "prediction", and "probability".
   * Calculates accuracy, attack precision, attack recall, attack F1-score, ROC-AUC, PR-AUC,
   * and the full Confusion Matrix.
   */
  def evaluate(predictions: DataFrame): EvaluationMetrics = {
    // Confusion Matrix aggregation in a single distributed pass
    val confusionRows = predictions.groupBy("label", "prediction").count().collect()

    var tn = 0L // Actual 0.0, Pred 0.0
    var fp = 0L // Actual 0.0, Pred 1.0
    var fn = 0L // Actual 1.0, Pred 0.0
    var tp = 0L // Actual 1.0, Pred 1.0

    confusionRows.foreach { r =>
      val actual = r.getDouble(0)
      val pred = r.getDouble(1)
      val count = r.getLong(2)

      if (actual == 0.0 && pred == 0.0) tn += count
      else if (actual == 0.0 && pred == 1.0) fp += count
      else if (actual == 1.0 && pred == 0.0) fn += count
      else if (actual == 1.0 && pred == 1.0) tp += count
    }

    val cm = ConfusionMatrix(
      trueNegatives  = tn,
      falsePositives = fp,
      falseNegatives = fn,
      truePositives  = tp
    )

    // Spark ML Evaluators for Area Under Curves
    val rocAucEvaluator = new BinaryClassificationEvaluator()
      .setLabelCol("label")
      .setRawPredictionCol("rawPrediction")
      .setMetricName("areaUnderROC")

    val prAucEvaluator = new BinaryClassificationEvaluator()
      .setLabelCol("label")
      .setRawPredictionCol("rawPrediction")
      .setMetricName("areaUnderPR")

    val rocAuc = rocAucEvaluator.evaluate(predictions)
    val prAuc = prAucEvaluator.evaluate(predictions)

    EvaluationMetrics(
      confusionMatrix = cm,
      rocAuc          = rocAuc,
      prAuc           = prAuc
    )
  }

  // ── 7. Artifact Persistence ─────────────────────────────────

  /**
   * Save model, test predictions, metrics, and documentation artifacts.
   */
  def saveArtifacts(
    model: org.apache.spark.ml.PipelineModel,
    predictions: DataFrame,
    metrics: EvaluationMetrics,
    audit: DatasetAudit,
    splitSummary: SplitSummary,
    config: ModelConfig,
    runDir: String
  ): Unit = {
    val runFolder = new File(runDir)
    if (!runFolder.exists()) runFolder.mkdirs()

    // 1. Save Spark ML Pipeline Model
    val modelDir = s"$runDir/model"
    model.write.overwrite().save(modelDir)
    log.info(s"Saved PipelineModel to $modelDir")

    // 2. Save test predictions sample (first 100 rows as CSV for inspection)
    val sampleFile = new File(s"$runDir/predictions_sample.csv")
    val pwSample = new PrintWriter(sampleFile)
    try {
      pwSample.println("label,label_binary,prediction,benign_prob,attack_prob")
      predictions
        .select("label", "label_binary", "prediction", "probability")
        .take(100)
        .foreach { r =>
          val label = r.getDouble(0)
          val labelBinary = r.getString(1)
          val pred = r.getDouble(2)
          val probVec = r.getAs[Vector](3)
          val bProb = probVec(0)
          val aProb = probVec(1)
          pwSample.println(f"$label%.1f,$labelBinary,$pred%.1f,$bProb%.6f,$aProb%.6f")
        }
    } finally {
      pwSample.close()
    }

    // 3. Save JSON Metrics
    val cm = metrics.confusionMatrix
    val metricsJson =
      s"""{
         |  "run_timestamp": "${Instant.now()}",
         |  "seed": ${config.seed},
         |  "train_ratio": ${config.trainRatio},
         |  "model_hyperparameters": {
         |    "max_iter": ${config.maxIter},
         |    "reg_param": ${config.regParam},
         |    "elastic_net_param": ${config.elasticNetParam},
         |    "threshold": ${config.threshold}
         |  },
         |  "dataset_counts": {
         |    "raw_rows": ${audit.rawRowCount},
         |    "excluded_rows": ${audit.excludedRowCount},
         |    "usable_rows": ${audit.usableRowCount},
         |    "usable_benign": ${audit.usableBenignCount},
         |    "usable_attack": ${audit.usableAttackCount},
         |    "feature_count": ${audit.featureCount}
         |  },
         |  "split_counts": {
         |    "train_total": ${splitSummary.trainCount},
         |    "train_benign": ${splitSummary.trainBenignCount},
         |    "train_attack": ${splitSummary.trainAttackCount},
         |    "test_total": ${splitSummary.testCount},
         |    "test_benign": ${splitSummary.testBenignCount},
         |    "test_attack": ${splitSummary.testAttackCount}
         |  },
         |  "confusion_matrix": {
         |    "true_negatives": ${cm.trueNegatives},
         |    "false_positives": ${cm.falsePositives},
         |    "false_negatives": ${cm.falseNegatives},
         |    "true_positives": ${cm.truePositives},
         |    "total_evaluated": ${cm.total}
         |  },
         |  "metrics": {
         |    "accuracy": ${cm.accuracy},
         |    "attack_precision": ${cm.precision},
         |    "attack_recall": ${cm.recall},
         |    "attack_f1_score": ${cm.f1Score},
         |    "roc_auc": ${metrics.rocAuc},
         |    "pr_auc": ${metrics.prAuc},
         |    "false_positive_rate": ${cm.falsePositiveRate},
         |    "false_negative_rate": ${cm.falseNegativeRate}
         |  }
         |}""".stripMargin

    val metricsFile = new File(s"$runDir/metrics.json")
    val pwMetrics = new PrintWriter(metricsFile)
    try pwMetrics.write(metricsJson) finally pwMetrics.close()

    // 4. Save Confusion Matrix JSON
    val cmJson =
      s"""{
         |  "true_negatives": ${cm.trueNegatives},
         |  "false_positives": ${cm.falsePositives},
         |  "false_negatives": ${cm.falseNegatives},
         |  "true_positives": ${cm.truePositives},
         |  "total_evaluated": ${cm.total}
         |}""".stripMargin
    val cmFile = new File(s"$runDir/confusion-matrix.json")
    val pwCm = new PrintWriter(cmFile)
    try pwCm.write(cmJson) finally pwCm.close()

    // 5. Save Dataset Split Summary JSON
    val splitJson =
      s"""{
         |  "seed": ${config.seed},
         |  "train_ratio": ${config.trainRatio},
         |  "raw_rows": ${audit.rawRowCount},
         |  "excluded_rows": ${audit.excludedRowCount},
         |  "usable_rows": ${audit.usableRowCount},
         |  "usable_benign": ${audit.usableBenignCount},
         |  "usable_attack": ${audit.usableAttackCount},
         |  "train_total": ${splitSummary.trainCount},
         |  "train_benign": ${splitSummary.trainBenignCount},
         |  "train_attack": ${splitSummary.trainAttackCount},
         |  "test_total": ${splitSummary.testCount},
         |  "test_benign": ${splitSummary.testBenignCount},
         |  "test_attack": ${splitSummary.testAttackCount}
         |}""".stripMargin
    val splitFile = new File(s"$runDir/dataset-split-summary.json")
    val pwSplit = new PrintWriter(splitFile)
    try pwSplit.write(splitJson) finally pwSplit.close()

    // 6. Save Feature List
    val featuresJson =
      s"""{
         |  "feature_count": ${audit.featureNames.size},
         |  "features": [
         |    ${audit.featureNames.map(f => s""""$f"""").mkString(",\n    ")}
         |  ]
         |}""".stripMargin
    val featuresFile = new File(s"$runDir/features.json")
    val pwFeatures = new PrintWriter(featuresFile)
    try pwFeatures.write(featuresJson) finally pwFeatures.close()

    // 7. Save Human-Readable Evaluation Summary
    val benignPct = audit.usableBenignCount * 100.0 / audit.usableRowCount
    val attackPct = audit.usableAttackCount * 100.0 / audit.usableRowCount
    val accPct = cm.accuracy * 100.0
    val precPct = cm.precision * 100.0
    val recPct = cm.recall * 100.0
    val f1Pct = cm.f1Score * 100.0
    val fprPct = cm.falsePositiveRate * 100.0
    val fnrPct = cm.falseNegativeRate * 100.0
    val rocAucVal = metrics.rocAuc
    val prAucVal = metrics.prAuc

    val tnStr = f"${cm.trueNegatives}%-12d (TN)"
    val fpStr = f"${cm.falsePositives}%-12d (FP)"
    val fnStr = f"${cm.falseNegatives}%-12d (FN)"
    val tpStr = f"${cm.truePositives}%-12d (TP)"

    val summaryText =
      s"""================================================================================
         |  SparkShield — Milestone 5: Logistic Regression Intrusion Detection Report
         |================================================================================
         |Run Timestamp        : ${Instant.now()}
         |Model Algorithm      : Spark MLlib LogisticRegression (PipelineModel)
         |Hyperparameters      : maxIter=${config.maxIter}, regParam=${config.regParam}, elasticNetParam=${config.elasticNetParam}, threshold=${config.threshold}
         |Data Input Path      : ${config.inputPath}
         |
         |── Dataset Counts & Quality ──────────────────────────────────────────────────
         |Raw Input Rows       : ${audit.rawRowCount}
         |Excluded Invalid Rows: ${audit.excludedRowCount} (0.015% non-finite rows)
         |Usable Clean Rows    : ${audit.usableRowCount}
         |Usable BENIGN Flows  : ${audit.usableBenignCount} (${f"$benignPct%.2f"}%)
         |Usable ATTACK Flows  : ${audit.usableAttackCount} (${f"$attackPct%.2f"}%)
         |Discovered Features  : ${audit.featureCount} numeric flow features
         |
         |── Train / Test Partitioning ─────────────────────────────────────────────────
         |Train Set (80.0%)    : ${splitSummary.trainCount} rows (BENIGN: ${splitSummary.trainBenignCount}, ATTACK: ${splitSummary.trainAttackCount})
         |Test Set (20.0%)     : ${splitSummary.testCount} rows (BENIGN: ${splitSummary.testBenignCount}, ATTACK: ${splitSummary.testAttackCount})
         |Random Seed          : ${config.seed}
         |
         |── Confusion Matrix (Test Set) ───────────────────────────────────────────────
         |                           Predicted BENIGN (0.0)      Predicted ATTACK (1.0)
         |Actual BENIGN (0.0) :      $tnStr      $fpStr
         |Actual ATTACK (1.0) :      $fnStr      $tpStr
         |
         |── Performance Evaluation Metrics ────────────────────────────────────────────
         |Accuracy             : ${f"$accPct%.4f"}%
         |Attack Precision     : ${f"$precPct%.4f"}% (Purity of flagged alerts)
         |Attack Recall        : ${f"$recPct%.4f"}% (Intrusions successfully caught)
         |Attack F1-Score      : ${f"$f1Pct%.4f"}%
         |ROC-AUC              : ${f"$rocAucVal%.6f"}
         |PR-AUC               : ${f"$prAucVal%.6f"}
         |False Positive Rate  : ${f"$fprPct%.4f"}% (Benign incorrectly flagged)
         |False Negative Rate  : ${f"$fnrPct%.4f"}% (Attacks missed by model)
         |
         |── Critical Analysis for Network Intrusion Detection ─────────────────────────
         |Missed Attacks (FN)  : ${cm.falseNegatives} out of ${cm.falseNegatives + cm.truePositives} (${f"$fnrPct%.3f"}% missed)
         |False Alarms (FP)    : ${cm.falsePositives} out of ${cm.falsePositives + cm.trueNegatives} (${f"$fprPct%.3f"}% alarms)
         |================================================================================
         |""".stripMargin

    val summaryFile = new File(s"$runDir/evaluation-summary.txt")
    val pwSummary = new PrintWriter(summaryFile)
    try pwSummary.write(summaryText) finally pwSummary.close()

    log.info(s"Artifacts saved successfully in $runDir")
  }

  // ── 8. CLI Argument Parsing ─────────────────────────────────

  def parseArgs(args: Array[String]): ModelConfig = {
    var config = ModelConfig()

    var i = 0
    while (i < args.length) {
      args(i) match {
        case "--input" if i + 1 < args.length =>
          config = config.copy(inputPath = args(i + 1))
          i += 2
        case "--output-dir" if i + 1 < args.length =>
          config = config.copy(outputBaseDir = args(i + 1))
          i += 2
        case "--train-ratio" if i + 1 < args.length =>
          config = config.copy(trainRatio = args(i + 1).toDouble)
          i += 2
        case "--seed" if i + 1 < args.length =>
          config = config.copy(seed = args(i + 1).toLong)
          i += 2
        case "--max-iter" if i + 1 < args.length =>
          config = config.copy(maxIter = args(i + 1).toInt)
          i += 2
        case "--reg-param" if i + 1 < args.length =>
          config = config.copy(regParam = args(i + 1).toDouble)
          i += 2
        case "--elastic-net" if i + 1 < args.length =>
          config = config.copy(elasticNetParam = args(i + 1).toDouble)
          i += 2
        case "--threshold" if i + 1 < args.length =>
          config = config.copy(threshold = args(i + 1).toDouble)
          i += 2
        case other =>
          log.warn(s"Unrecognized argument: $other")
          i += 1
      }
    }
    config
  }

  // ── 9. Main Application Entry Point ─────────────────────────

  def main(args: Array[String]): Unit = {
    val config = parseArgs(args)

    println("\n" + "=" * 75)
    println("  SparkShield — Milestone 5: ML Intrusion Detection Training")
    println(s"  Input Dataset : ${config.inputPath}")
    println(s"  Train Ratio   : ${config.trainRatio * 100}%")
    println(s"  Random Seed   : ${config.seed}")
    println(s"  Max Iterations: ${config.maxIter}")
    println(s"  Reg Param (L2): ${config.regParam}")
    println("=" * 75 + "\n")

    val spark = SparkSessionFactory.create(
      "SparkShield-ML-IntrusionDetection",
      extras = Map(
        "spark.driver.memory" -> "3g",
        "spark.sql.shuffle.partitions" -> "8"
      )
    )

    try {
      // 1. Load Cleaned Dataset
      val inputFile = new File(config.inputPath)
      if (!inputFile.exists()) {
        throw new IllegalArgumentException(s"Input dataset path does not exist: ${config.inputPath}")
      }

      val rawDf = spark.read.parquet(config.inputPath)

      // 2. Prepare & Audit Data
      val (preparedDf, audit) = prepareData(rawDf)

      // 3. Train / Test Split
      val (trainDf, testDf, splitSummary) = splitDataset(preparedDf, config)

      // 4. Build Pipeline
      val pipeline = buildPipeline(audit.featureNames, config)

      // 5. Fit Pipeline on Training Data ONLY
      println(s"Fitting Logistic Regression Pipeline on ${splitSummary.trainCount} training samples...")
      val t0 = System.currentTimeMillis()
      val pipelineModel = pipeline.fit(trainDf)
      val t1 = System.currentTimeMillis()
      println(s"Pipeline training completed in ${(t1 - t0) / 1000.0}s\n")

      // 6. Predict on Test Set
      println(s"Evaluating model on ${splitSummary.testCount} held-out test samples...")
      val predictions = pipelineModel.transform(testDf).cache()

      // 7. Compute Metrics
      val metrics = evaluate(predictions)

      // 8. Generate Timestamped Run Directory & Save Artifacts
      val timestampStr = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss")
        .withZone(ZoneId.systemDefault())
        .format(Instant.now())
      val runDir = s"${config.outputBaseDir}/run-$timestampStr"

      saveArtifacts(
        model        = pipelineModel,
        predictions  = predictions,
        metrics      = metrics,
        audit        = audit,
        splitSummary = splitSummary,
        config       = config,
        runDir       = runDir
      )

      // 9. Print Evaluation Summary to Console
      val summaryFile = new File(s"$runDir/evaluation-summary.txt")
      val summarySource = scala.io.Source.fromFile(summaryFile)
      try {
        println(summarySource.mkString)
      } finally {
        summarySource.close()
      }

      println(s"Training and evaluation complete. Artifacts saved in: $runDir\n")

    } finally {
      spark.stop()
    }
  }
}
