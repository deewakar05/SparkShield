# SparkShield — Milestone 1 Actual Test Results
# Generated: 2026-10-23 23:40 IST
# Machine: MacBook Air M1, macOS 27.2
# Java: Zulu 8.96 (1.8.0_504) | Scala: 2.12.18 | Spark: 3.5.9 | sbt: 1.10.7

## Environment Check (check-env.sh)

```
Results: 23 passed, 0 warnings, 0 failures
All checks passed. Ready to build.
```

## Compile

```
[info] welcome to sbt 1.10.7 (Azul Systems, Inc. Java 1.8.0_504)
[info] compiling 2 Scala sources to .../target/scala-2.12/classes ...
[info] done compiling
[success] Total time: 4 s, completed 23 Oct, 2026 11:38:59 PM
```

## Assembly

```
[info] Built: .../sparkshield-assembly-1.0.0.jar
[info] Jar hash: b28f09aa986a4e774a3d3a1f981def9735dfa709
[success] Total time: 1 s, completed 23 Oct, 2026 11:39:07 PM
```

## RDDBasics spark-submit Output (actual)

```
======================================================================
  SparkShield — RDD Basics Demonstration
  Spark version : 3.5.9
  Scala version : version 2.12.18
======================================================================

── 1. RDD Creation ──────────────────────────────────────────────
  Created RDD with 100 elements across 4 partitions
  Word RDD: 10 words, 2 partitions

── 2. Transformations ───────────────────────────────────────────
  Squares count : 100
  Even numbers  : 50
  Total characters from word RDD: 65
  Distinct word count : 8
  After union         : 11
  Per-partition sizes : partition_size=25, partition_size=25, partition_size=25, partition_size=25
  After repartition(2): 2 partitions

── 3. Key-Value Aggregations ────────────────────────────────────
  Counts by (value mod 5) using reduceByKey:
    Remainder 0 -> 20 records
    Remainder 1 -> 20 records
    Remainder 2 -> 20 records
    Remainder 3 -> 20 records
    Remainder 4 -> 20 records
  Same result via groupByKey (note: less efficient at scale):
    Remainder 0 -> 20 records
    Remainder 1 -> 20 records
    Remainder 2 -> 20 records
    Remainder 3 -> 20 records
    Remainder 4 -> 20 records

  Word frequencies:
    spark : 3
    transform : 1
    action : 1
    bigdata : 1
    network : 1
    rdd : 1
    intrusion : 1
    detection : 1

── 4. Actions ────────────────────────────────────────────────────
  count()          : 100
  first()          : 1
  take(5)          : 1, 2, 3, 4, 5
  reduce(+)        : 5050
  countByKey (mod5): (0,20), (1,20), (2,20), (3,20), (4,20)
  foreach counted evens: 50

── 5. Broadcast Variables ───────────────────────────────────────
  Labelled remainder counts (using broadcast):
    divisible-by-5 : 20
    remainder-1 : 20
    remainder-2 : 20
    remainder-3 : 20
    remainder-4 : 20

── 6. Accumulators ──────────────────────────────────────────────
  Accumulator total (sum 1..100) : 5050
  Expected (n*(n+1)/2 for n=100) : 5050
  Multiples of 3 in 1..100 (acc) : 33
  Expected (33)                  : 33

── 7. Partition Inspection ───────────────────────────────────────
  Default parallelism : 8
  numbers partitions  : 4
  words partitions    : 2
  elements per partition index:
    partition[0] -> 25 elements, first=1
    partition[1] -> 25 elements, first=26
    partition[2] -> 25 elements, first=51
    partition[3] -> 25 elements, first=76

======================================================================
  RDD Basics — COMPLETE
======================================================================
```

## Unit Tests (RDDBasicsSpec)

```
[info] RDDBasicsSpec:
[info] - parallelize creates RDD with correct count
[info] - filter transformation keeps only even numbers
[info] - map transformation squares each element
[info] - flatMap expands each word into characters
[info] - reduceByKey computes correct word frequencies
[info] - reduce sums 1 to 100 correctly
[info] - distinct removes duplicates
[info] - union combines two RDDs
[info] - repartition changes partition count
[info] - broadcast variable is accessible inside transformation
[info] - accumulator counts elements correctly
[info] - countByKey returns correct counts for remainder groups
[info] - sortByKey produces ascending order
[info] - mapPartitionsWithIndex enumerates partitions correctly
[info] Run completed in 3 seconds, 174 milliseconds.
[info] Total number of tests run: 14
[info] Suites: completed 1, aborted 0
[info] Tests: succeeded 14, failed 0, canceled 0, ignored 0, pending 0
[info] All tests passed.
[success] Total time: 9 s
```

## Summary

| Step | Status |
|------|--------|
| Environment check (23 items) | ✅ PASSED |
| sbt build.sbt loads | ✅ PASSED |
| sbt compile | ✅ PASSED |
| sbt assembly | ✅ PASSED |
| spark-submit RDDBasics | ✅ PASSED |
| sbt test (14 tests) | ✅ PASSED |
