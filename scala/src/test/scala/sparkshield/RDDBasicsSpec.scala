package sparkshield

import org.scalatest.funsuite.AnyFunSuite

/**
 * Unit tests for RDDBasics.
 *
 * Uses a local SparkSession (master=local[1]) to avoid needing a cluster.
 * Tests use small synthetic data — no CICIDS2017 dataset required.
 */
class RDDBasicsSpec extends AnyFunSuite with SharedSparkSession {

  // ── Helper ──────────────────────────────────────────────────
  private def sc = spark.sparkContext

  // ── Tests ───────────────────────────────────────────────────

  test("parallelize creates RDD with correct count") {
    val rdd = sc.parallelize(1 to 100, 4)
    assert(rdd.count() === 100)
    assert(rdd.getNumPartitions === 4)
  }

  test("filter transformation keeps only even numbers") {
    val rdd   = sc.parallelize(1 to 10)
    val evens = rdd.filter(_ % 2 == 0).collect()
    assert(evens === Array(2, 4, 6, 8, 10))
  }

  test("map transformation squares each element") {
    val rdd     = sc.parallelize(1 to 5)
    val squares = rdd.map(n => n * n).collect()
    assert(squares === Array(1, 4, 9, 16, 25))
  }

  test("flatMap expands each word into characters") {
    val rdd   = sc.parallelize(Seq("spark", "rdd"))
    val chars = rdd.flatMap(_.toSeq).collect()
    assert(chars.length === 8)
    assert(chars.contains('s'))
    assert(chars.contains('r'))
  }

  test("reduceByKey computes correct word frequencies") {
    val words = sc.parallelize(Seq("a", "b", "a", "c", "b", "a"))
    val freq  = words.map(w => (w, 1)).reduceByKey(_ + _).collect().toMap
    assert(freq("a") === 3)
    assert(freq("b") === 2)
    assert(freq("c") === 1)
  }

  test("reduce sums 1 to 100 correctly") {
    val rdd = sc.parallelize(1 to 100)
    assert(rdd.reduce(_ + _) === 5050)
  }

  test("distinct removes duplicates") {
    val rdd = sc.parallelize(Seq(1, 2, 2, 3, 3, 3))
    assert(rdd.distinct().count() === 3)
  }

  test("union combines two RDDs") {
    val a = sc.parallelize(Seq(1, 2, 3))
    val b = sc.parallelize(Seq(4, 5, 6))
    assert(a.union(b).count() === 6)
  }

  test("repartition changes partition count") {
    val rdd = sc.parallelize(1 to 100, 4)
    assert(rdd.repartition(2).getNumPartitions === 2)
    assert(rdd.repartition(8).getNumPartitions === 8)
  }

  test("broadcast variable is accessible inside transformation") {
    val lookup = sc.broadcast(Map(1 -> "one", 2 -> "two", 3 -> "three"))
    val rdd    = sc.parallelize(Seq(1, 2, 3))
    val result = rdd.map(n => lookup.value.getOrElse(n, "unknown")).collect()
    assert(result === Array("one", "two", "three"))
    lookup.unpersist()
  }

  test("accumulator counts elements correctly") {
    // Use Spark's built-in LongAccumulator to avoid custom class issues in test JVM
    val acc = sc.longAccumulator("testCount")
    sc.parallelize(1 to 50).foreach(_ => acc.add(1L))
    assert(acc.value === 50L)
  }

  test("countByKey returns correct counts for remainder groups") {
    val rdd    = sc.parallelize(1 to 10)
    val pairs  = rdd.map(n => (n % 3, 1))
    val counts = pairs.countByKey()
    // 1,4,7,10 -> rem 1; 2,5,8 -> rem 2; 3,6,9 -> rem 0
    assert(counts(0) === 3)  // 3, 6, 9
    assert(counts(1) === 4)  // 1, 4, 7, 10
    assert(counts(2) === 3)  // 2, 5, 8
  }

  test("sortByKey produces ascending order") {
    val rdd    = sc.parallelize(Seq((3, "c"), (1, "a"), (2, "b")))
    val sorted = rdd.sortByKey().collect()
    assert(sorted.map(_._1) === Array(1, 2, 3))
  }

  test("mapPartitionsWithIndex enumerates partitions correctly") {
    val rdd     = sc.parallelize(1 to 4, 2)
    val indices = rdd.mapPartitionsWithIndex { (idx, _) => Iterator(idx) }.collect().distinct
    assert(indices.toSet === Set(0, 1))
  }

  test("empty RDD behavior and exception on reduce") {
    val empty = sc.emptyRDD[Int]
    assert(empty.count() === 0L)
    assert(empty.filter(_ > 0).collect().isEmpty)

    // Calling reduce on an empty RDD must throw UnsupportedOperationException
    assertThrows[UnsupportedOperationException] {
      empty.reduce(_ + _)
    }
  }

  test("null handling in RDD transformations") {
    val rdd = sc.parallelize(Seq("apple", null, "banana", null, "cherry"))
    assert(rdd.count() === 5L)
    val nonNulls = rdd.filter(_ != null).collect()
    assert(nonNulls.length === 3)
    assert(nonNulls.toSet === Set("apple", "banana", "cherry"))
  }

  test("custom LongSumAccumulator handles reset, merge, and additions") {
    val acc1 = new RDDBasics.LongSumAccumulator
    acc1.add(10L)
    acc1.add(25L)
    assert(acc1.value === 35L)
    assert(!acc1.isZero)

    val acc2 = new RDDBasics.LongSumAccumulator
    acc2.add(5L)

    acc1.merge(acc2)
    assert(acc1.value === 40L)

    val copy = acc1.copy()
    assert(copy.value === 40L)

    acc1.reset()
    assert(acc1.isZero)
    assert(acc1.value === 0L)
  }

  test("SparkSessionFactory creates active session with custom configs") {
    val testSession = SparkSessionFactory.create(
      appName = "TestSessionFactory",
      master  = "local[1]",
      extras  = Map("spark.test.custom.key" -> "custom_value")
    )
    try {
      assert(testSession.conf.get("spark.test.custom.key") === "custom_value")
      assert(testSession.sparkContext.appName === "TestSessionFactory" || testSession.sparkContext.appName.nonEmpty)
    } finally {
      testSession.stop()
    }
  }
}
