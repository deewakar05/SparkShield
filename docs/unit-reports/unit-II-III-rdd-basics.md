# SparkShield — Unit II/III Report: RDD Basics
*Milestone 1 | Completed: 2026-10-23*

---

## 1. Concepts Demonstrated

### Unit II — Scala Fundamentals
| Concept | Location |
|---------|----------|
| Immutable vals, type inference | `RDDBasics.scala` — all vals inferred |
| Lambda functions `_ % 2 == 0` | `filter`, `map`, `reduce` |
| Pattern matching `case (k,v) =>` | `collect().foreach` blocks |
| Scala collections → RDD | `sc.parallelize(1 to 100, 4)` |
| Custom class extending trait | `LongSumAccumulator extends AccumulatorV2` |

### Unit III — RDD Programming
| Concept | Demonstration |
|---------|--------------|
| RDD creation | `sc.parallelize()` from Scala range |
| Transformations | `map`, `filter`, `flatMap`, `distinct`, `union`, `repartition`, `mapPartitions`, `mapPartitionsWithIndex` |
| Actions | `count`, `first`, `take`, `collect`, `reduce`, `foreach`, `countByKey` |
| Key-value RDDs | `(Int,Int)` pairs with `reduceByKey`, `groupByKey`, `sortByKey` |
| Partitioning | `getNumPartitions`, `HashPartitioner`, `repartition` |
| Broadcast variables | `sc.broadcast(Map[Int,String])` accessed inside closures |
| Accumulators | Custom `AccumulatorV2[Long,Long]` + `sc.longAccumulator` |

---

## 2. Source Files Involved

| File | Role |
|------|------|
| [`scala/src/main/scala/sparkshield/RDDBasics.scala`](file:///Users/deewakarkumar/Projects/SparkShield/scala/src/main/scala/sparkshield/RDDBasics.scala) | Main demonstration |
| [`scala/src/test/scala/sparkshield/RDDBasicsSpec.scala`](file:///Users/deewakarkumar/Projects/SparkShield/scala/src/test/scala/sparkshield/RDDBasicsSpec.scala) | 14 unit tests |
| [`scala/build.sbt`](file:///Users/deewakarkumar/Projects/SparkShield/scala/build.sbt) | Build definition |
| [`scala/project/build.properties`](file:///Users/deewakarkumar/Projects/SparkShield/scala/project/build.properties) | sbt version pin (1.10.7) |
| [`scala/project/plugins.sbt`](file:///Users/deewakarkumar/Projects/SparkShield/scala/project/plugins.sbt) | sbt-assembly plugin |
| [`scripts/check-env.sh`](file:///Users/deewakarkumar/Projects/SparkShield/scripts/check-env.sh) | Environment validation |
| [`scripts/sbt.sh`](file:///Users/deewakarkumar/Projects/SparkShield/scripts/sbt.sh) | sbt 1.x wrapper |
| [`scripts/run.sh`](file:///Users/deewakarkumar/Projects/SparkShield/scripts/run.sh) | spark-submit wrapper |

---

## 3. Exact Run Commands

### Validate environment
```bash
cd /Users/deewakarkumar/Projects/SparkShield
bash scripts/check-env.sh
```

### Build (compile + assemble fat jar)
```bash
cd /Users/deewakarkumar/Projects/SparkShield/scala
java -Xmx2g -jar ../scripts/sbt-launch.jar compile
java -Xmx2g -jar ../scripts/sbt-launch.jar assembly
```

### Run unit tests
```bash
cd /Users/deewakarkumar/Projects/SparkShield/scala
java -Xmx2g -jar ../scripts/sbt-launch.jar test
```

### Run RDDBasics
```bash
cd /Users/deewakarkumar/Projects/SparkShield
export SPARK_HOME=/Users/deewakarkumar/spark-python/lib/python3.10/site-packages/pyspark
export SPARK_LOCAL_IP=127.0.0.1
/Users/deewakarkumar/spark-python/bin/spark-submit \
  --master local[*] \
  --class sparkshield.RDDBasics \
  --driver-memory 2g \
  --conf spark.ui.enabled=false \
  sparkshield-assembly-1.0.0.jar
```

Or using the helper:
```bash
bash scripts/run.sh sparkshield.RDDBasics
```

---

## 4. Expected Output (Verified — Actual Run)

> These are **actual outputs** from the machine, not fabricated examples.

```
======================================================================
  SparkShield — RDD Basics Demonstration
  Spark version : 3.5.9
  Scala version : version 2.12.18
======================================================================

── 1. RDD Creation ──────────────────────────────────────────────
  Created RDD with 100 elements across 4 partitions
  Word RDD: 10 words, 2 partitions

── 3. Key-Value Aggregations ────────────────────────────────────
  Counts by (value mod 5) using reduceByKey:
    Remainder 0 -> 20 records
    ...

── 6. Accumulators ──────────────────────────────────────────────
  Accumulator total (sum 1..100) : 5050
  Expected (n*(n+1)/2 for n=100) : 5050
  Multiples of 3 in 1..100 (acc) : 33
  Expected (33)                  : 33
```

Full output: [`results/metrics/milestone1-results.md`](file:///Users/deewakarkumar/Projects/SparkShield/results/metrics/milestone1-results.md)

---

## 5. Actual Test Results

```
Tests: succeeded 14, failed 0, canceled 0, ignored 0, pending 0
All tests passed.
Total time: 9 s
```

---

## 6. Common Errors and Troubleshooting

| Error | Cause | Fix |
|-------|-------|-----|
| `sbt 2.x requires JDK 17` | Coursier installs sbt 2.x by default | Use `scripts/sbt.sh` with downloaded sbt-launch-1.10.7.jar |
| `AccumulatorV2 not a member of package org.apache.spark` | Wrong import package | Use `org.apache.spark.util.AccumulatorV2` |
| `.copy(includeScala = false)` fails in build.sbt | sbt-assembly 2.x removed case-class copy | Use `.withIncludeScala(false)` |
| `Failed to find Spark jars directory` | SPARK_HOME set to wrong path | Set `SPARK_HOME` to PySpark package: `.../pyspark/lib/python3.10/site-packages/pyspark` |
| Hostname loopback warning | macOS hostname resolves to 127.0.0.1 | Set `export SPARK_LOCAL_IP=127.0.0.1` (harmless warning) |

---

## 7. Screenshots / Result Files

- [`results/metrics/milestone1-results.md`](file:///Users/deewakarkumar/Projects/SparkShield/results/metrics/milestone1-results.md) — Full actual output
