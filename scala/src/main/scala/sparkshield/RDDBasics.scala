package sparkshield

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{HashPartitioner, SparkContext}
import org.slf4j.LoggerFactory

// ============================================================
// RDDBasics — Milestone 1
//
// Demonstrates Unit II / III concepts:
//   - RDD creation from a Scala collection (parallelize)
//   - Transformations: map, filter, flatMap, mapPartitions,
//     distinct, union, reduceByKey, groupByKey, sortByKey,
//     repartition
//   - Actions: count, first, take, collect, reduce, foreach,
//     countByKey, saveAsTextFile
//   - Key-value aggregation (reduceByKey vs groupByKey trade-offs)
//   - Partitioning and HashPartitioner
//   - Broadcast variables
//   - Accumulators (custom LongAccumulator)
//   - Simple partition-level operation via mapPartitions
//
// Run:
//   ./scripts/run.sh sparkshield.RDDBasics
// ============================================================
object RDDBasics {

  private val log = LoggerFactory.getLogger(getClass)

  // ── Custom Accumulator for demonstration ──────────────────
  class LongSumAccumulator extends AccumulatorV2[Long, Long] {
    private var _sum: Long = 0L
    override def isZero: Boolean        = _sum == 0L
    override def copy(): LongSumAccumulator = { val acc = new LongSumAccumulator; acc._sum = _sum; acc }
    override def reset(): Unit          = { _sum = 0L }
    override def add(v: Long): Unit     = { _sum += v }
    override def merge(other: AccumulatorV2[Long, Long]): Unit = { _sum += other.value }
    override def value: Long            = _sum
  }

  def main(args: Array[String]): Unit = {

    log.info("SparkShield — RDDBasics starting")

    val spark = SparkSession
      .builder()
      .appName("SparkShield-RDD-Basics")
      .master("local[*]")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.memory", "1g")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    val sc: SparkContext = spark.sparkContext

    println("\n" + "=" * 70)
    println("  SparkShield — RDD Basics Demonstration")
    println("  Spark version : " + spark.version)
    println("  Scala version : " + scala.util.Properties.versionString)
    println("=" * 70 + "\n")

    // ── 1. RDD CREATION ──────────────────────────────────────
    println("── 1. RDD Creation ──────────────────────────────────────────────")
    val numbers: RDD[Int] = sc.parallelize(1 to 100, numSlices = 4)
    println(s"  Created RDD with ${numbers.count()} elements across ${numbers.getNumPartitions} partitions")

    val words: RDD[String] = sc.parallelize(
      Seq("spark", "rdd", "transform", "spark", "action",
          "network", "intrusion", "detection", "spark", "bigdata"),
      numSlices = 2
    )
    println(s"  Word RDD: ${words.count()} words, ${words.getNumPartitions} partitions")

    // ── 2. TRANSFORMATIONS ───────────────────────────────────
    println("\n── 2. Transformations ───────────────────────────────────────────")

    // map / filter
    val squares = numbers.map(n => n * n)
    val evens   = numbers.filter(_ % 2 == 0)
    println(s"  Squares count : ${squares.count()}")
    println(s"  Even numbers  : ${evens.count()}")

    // flatMap
    val chars: RDD[Char] = words.flatMap(w => w.toSeq)
    println(s"  Total characters from word RDD: ${chars.count()}")

    // distinct / union
    val distinctWords = words.distinct()
    val extraWords    = sc.parallelize(Seq("anomaly", "packet", "spark"), 2)
    val allWords      = distinctWords.union(extraWords)
    println(s"  Distinct word count : ${distinctWords.count()}")
    println(s"  After union         : ${allWords.count()}")

    // mapPartitions — count elements per partition
    val perPartitionCounts: RDD[String] = numbers.mapPartitions { iter =>
      val count = iter.length
      Iterator(s"partition_size=$count")
    }
    println(s"  Per-partition sizes : ${perPartitionCounts.collect().mkString(", ")}")

    // repartition
    val repartitioned = numbers.repartition(2)
    println(s"  After repartition(2): ${repartitioned.getNumPartitions} partitions")

    // ── 3. KEY-VALUE TRANSFORMATIONS AND AGGREGATIONS ───────
    println("\n── 3. Key-Value Aggregations ────────────────────────────────────")

    // Map each number to (remainder mod 5, 1) then reduceByKey
    val modPairs: RDD[(Int, Int)] = numbers.map(n => (n % 5, 1))

    // reduceByKey — efficient: combines locally before shuffle
    val countByRemainder = modPairs.reduceByKey(_ + _).sortByKey()
    println("  Counts by (value mod 5) using reduceByKey:")
    countByRemainder.collect().foreach { case (k, v) =>
      println(s"    Remainder $k -> $v records")
    }

    // groupByKey — shown for comparison; less efficient than reduceByKey
    // for large data because it shuffles all values before aggregation
    val grouped = modPairs.groupByKey(new HashPartitioner(5))
    val groupedSizes = grouped.map { case (k, vs) => (k, vs.size) }.sortByKey()
    println("  Same result via groupByKey (note: less efficient at scale):")
    groupedSizes.collect().foreach { case (k, v) =>
      println(s"    Remainder $k -> $v records")
    }

    // Word frequency on word RDD
    val wordFreq = words.map(w => (w, 1)).reduceByKey(_ + _).sortBy(_._2, ascending = false)
    println("\n  Word frequencies:")
    wordFreq.collect().foreach { case (w, c) => println(s"    $w : $c") }

    // ── 4. ACTIONS ───────────────────────────────────────────
    println("\n── 4. Actions ────────────────────────────────────────────────────")

    println(s"  count()          : ${numbers.count()}")
    println(s"  first()          : ${numbers.first()}")
    println(s"  take(5)          : ${numbers.take(5).mkString(", ")}")
    println(s"  reduce(+)        : ${numbers.reduce(_ + _)}")
    println(s"  countByKey (mod5): ${modPairs.countByKey().toSeq.sortBy(_._1).mkString(", ")}")

    // foreach side-effect — counts evens
    var evenCountLocal = 0
    evens.collect().foreach(_ => evenCountLocal += 1)
    println(s"  foreach counted evens: $evenCountLocal")

    // ── 5. BROADCAST VARIABLES ───────────────────────────────
    println("\n── 5. Broadcast Variables ───────────────────────────────────────")
    val lookupTable: Map[Int, String] = Map(
      0 -> "divisible-by-5",
      1 -> "remainder-1",
      2 -> "remainder-2",
      3 -> "remainder-3",
      4 -> "remainder-4"
    )
    val broadcastLookup = sc.broadcast(lookupTable)

    val labelledCounts = countByRemainder.map { case (k, v) =>
      val label = broadcastLookup.value.getOrElse(k, "unknown")
      s"$label : $v"
    }
    println("  Labelled remainder counts (using broadcast):")
    labelledCounts.collect().foreach(s => println(s"    $s"))

    // ── 6. ACCUMULATORS ──────────────────────────────────────
    println("\n── 6. Accumulators ──────────────────────────────────────────────")
    val sumAcc = new LongSumAccumulator
    sc.register(sumAcc, "totalSum")

    numbers.foreach(n => sumAcc.add(n.toLong))
    println(s"  Accumulator total (sum 1..100) : ${sumAcc.value}")
    println(s"  Expected (n*(n+1)/2 for n=100) : ${100L * 101L / 2L}")

    // Count multiples of 3 using accumulator
    val mult3Acc = new LongSumAccumulator
    sc.register(mult3Acc, "mult3Count")
    numbers.filter(_ % 3 == 0).foreach(_ => mult3Acc.add(1L))
    println(s"  Multiples of 3 in 1..100 (acc) : ${mult3Acc.value}")
    println(s"  Expected (33)                  : 33")

    // ── 7. PARTITION INSPECTION ──────────────────────────────
    println("\n── 7. Partition Inspection ───────────────────────────────────────")
    println(s"  Default parallelism : ${sc.defaultParallelism}")
    println(s"  numbers partitions  : ${numbers.getNumPartitions}")
    println(s"  words partitions    : ${words.getNumPartitions}")

    // Partition contents (small RDD, safe to collect)
    println("  elements per partition index:")
    numbers.mapPartitionsWithIndex { (idx, iter) =>
      val elems = iter.toList
      Iterator(s"    partition[$idx] -> ${elems.size} elements, first=${elems.headOption.getOrElse("n/a")}")
    }.collect().foreach(println)

    // ── 8. SUMMARY ───────────────────────────────────────────
    println("\n" + "=" * 70)
    println("  RDD Basics — COMPLETE")
    println("=" * 70 + "\n")

    broadcastLookup.unpersist()
    spark.stop()
    log.info("SparkShield — RDDBasics finished")
  }
}
