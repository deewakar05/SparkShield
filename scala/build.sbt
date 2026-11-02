// ============================================================
// SparkShield — sbt build definition
// Scala 2.12.18 | Spark 3.5.9 | sbt 1.10.7 | Java 8 (Zulu)
// ============================================================

ThisBuild / organization := "sparkshield"
ThisBuild / version      := "1.0.0"
ThisBuild / scalaVersion := "2.12.18"

ThisBuild / versionScheme := Some("semver-spec")

lazy val root = (project in file("."))
  .settings(
    name := "sparkshield",

    scalacOptions ++= Seq(
      "-encoding", "UTF-8",
      "-target:jvm-1.8",
      "-deprecation",
      "-feature",
      "-unchecked",
      "-Xlint"
    ),

    libraryDependencies ++= Seq(
      // Provided — already on the spark-submit classpath
      "org.apache.spark" %% "spark-core"      % "3.5.9" % "provided",
      "org.apache.spark" %% "spark-sql"       % "3.5.9" % "provided",
      "org.apache.spark" %% "spark-mllib"     % "3.5.9" % "provided",
      "org.apache.spark" %% "spark-graphx"    % "3.5.9" % "provided",

      // Kafka connector for Structured Streaming and Kafka Producer
      "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.5.9",
      "org.apache.kafka" % "kafka-clients" % "3.5.1",

      "org.slf4j" % "slf4j-api" % "1.7.36",

      // Test — not provided; need full jars in test runner
      "org.scalatest" %% "scalatest"   % "3.2.18" % Test,
      "org.apache.spark" %% "spark-core"   % "3.5.9" % Test,
      "org.apache.spark" %% "spark-sql"    % "3.5.9" % Test,
      "org.apache.spark" %% "spark-mllib"  % "3.5.9" % Test,
      "org.apache.spark" %% "spark-graphx" % "3.5.9" % Test
    ),

    // ── sbt-assembly fat-jar configuration ──────────────────
    assembly / assemblyJarName := "sparkshield-assembly-1.0.0.jar",

    // sbt-assembly 2.x API: withIncludeScala (not .copy())
    assembly / assemblyOption := (assembly / assemblyOption).value
      .withIncludeScala(false),

    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", "services", _*) => MergeStrategy.concat
      case PathList("META-INF", _*)             => MergeStrategy.discard
      case PathList("reference.conf")           => MergeStrategy.concat
      case _                                    => MergeStrategy.first
    },

    // Place assembled jar in project root for easy spark-submit
    assembly / assemblyOutputPath := baseDirectory.value / ".." / "sparkshield-assembly-1.0.0.jar",

    // Fork test JVM to avoid Spark SparkContext singleton conflicts
    Test / fork := true,
    Test / javaOptions ++= Seq(
      "-Dspark.master=local[1]",
      "-Dspark.ui.enabled=false",
      "-Xmx2g"
    )
  )
