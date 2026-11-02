package sparkshield

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.nio.file.Files

class NetworkGraphAnalyticsSpec extends AnyFunSuite with Matchers with SharedSparkSession {

  private val testSchema = StructType(Seq(
    StructField("destination_port", IntegerType, nullable = true),
    StructField("label_binary", StringType, nullable = true),
    StructField("has_invalid_numeric", BooleanType, nullable = true),
    StructField("fwd_header_length", DoubleType, nullable = true),
    StructField("init_win_bytes_forward", DoubleType, nullable = true),
    StructField("min_seg_size_forward", DoubleType, nullable = true),
    StructField("total_length_of_fwd_packets", DoubleType, nullable = true),
    StructField("total_length_of_bwd_packets", DoubleType, nullable = true),
    StructField("total_fwd_packets", DoubleType, nullable = true),
    StructField("total_backward_packets", DoubleType, nullable = true)
  ))

  private def createSyntheticDf() = {
    val rows = Seq(
      // 3 flows targeting Port 80 (2 Attacks, 1 Benign)
      Row(80, "ATTACK", false, 40.0, 32.0, 20.0, 100.0, 200.0, 2.0, 3.0),
      Row(80, "ATTACK", false, 40.0, 32.0, 20.0, 150.0, 250.0, 3.0, 4.0),
      Row(80, "BENIGN", false, 20.0, 64.0, 20.0, 50.0, 50.0, 1.0, 1.0),
      // 2 flows targeting Port 443 (All Benign)
      Row(443, "BENIGN", false, 20.0, 64.0, 20.0, 300.0, 400.0, 5.0, 6.0),
      Row(443, "BENIGN", false, 32.0, 128.0, 32.0, 200.0, 300.0, 4.0, 5.0),
      // 1 flow targeting Port 22 (Attack)
      Row(22, "ATTACK", false, 40.0, 32.0, 20.0, 80.0, 90.0, 2.0, 2.0),
      // 1 invalid flow (has_invalid_numeric=true) -> should be skipped
      Row(8080, "ATTACK", true, 40.0, 32.0, 20.0, 100.0, 100.0, 2.0, 2.0),
      // 1 null port row -> should be skipped
      Row(null, "BENIGN", false, 20.0, 64.0, 20.0, 50.0, 50.0, 1.0, 1.0),
      // Out of bounds port rows -> should be skipped
      Row(-1, "ATTACK", false, 40.0, 32.0, 20.0, 80.0, 90.0, 2.0, 2.0),
      Row(70000, "BENIGN", false, 40.0, 32.0, 20.0, 80.0, 90.0, 2.0, 2.0)
    )
    val rdd = spark.sparkContext.parallelize(rows)
    spark.createDataFrame(rdd, testSchema)
  }

  test("buildGraph constructs valid Property Graph with correct vertex separation") {
    val df = createSyntheticDf()
    val graph = NetworkGraphAnalytics.buildGraph(df)

    val vertices = graph.vertices.collect()
    vertices.length should be > 0

    // Check client profile group vertices (IDs < PortVertexOffset)
    val clients = vertices.filter(_._1 < NetworkGraphAnalytics.PortVertexOffset)
    clients.length should be > 0
    clients.foreach { case (_, attr) =>
      attr.vertexType shouldBe "CLIENT_PROFILE_GROUP"
      attr.name should startWith ("ProfileGroup-")
      attr.port shouldBe -1
    }

    // Check port vertices (IDs >= PortVertexOffset)
    val ports = vertices.filter(_._1 >= NetworkGraphAnalytics.PortVertexOffset)
    val portNumbers = ports.map(_._2.port).toSet
    portNumbers should contain (80)
    portNumbers should contain (443)
    portNumbers should contain (22)
    // Invalid port 8080 (has_invalid_numeric) and out of range ports (-1, 70000) should NOT be in the graph
    portNumbers should not contain (8080)
    portNumbers should not contain (-1)
    portNumbers should not contain (70000)
  }

  test("computePortCentrality correctly calculates In-Degree, PageRank, and attack metrics") {
    val df = createSyntheticDf()
    val graph = NetworkGraphAnalytics.buildGraph(df)
    val centralityDf = NetworkGraphAnalytics.computePortCentrality(graph, spark)

    val rows = centralityDf.collect()
    rows.length shouldBe 3 // ports 80, 443, 22

    val port80 = rows.find(_.getAs[Int]("destination_port") == 80).get
    port80.getAs[Long]("total_flows") shouldBe 3
    port80.getAs[Long]("attack_flows") shouldBe 2
    port80.getAs[Long]("benign_flows") shouldBe 1
    port80.getAs[Double]("attack_ratio") shouldBe (2.0 / 3.0 +- 0.001)
    port80.getAs[Int]("in_degree") should be >= 1
    port80.getAs[Double]("pagerank_score") should be > 0.0

    val port443 = rows.find(_.getAs[Int]("destination_port") == 443).get
    port443.getAs[Long]("total_flows") shouldBe 2
    port443.getAs[Long]("attack_flows") shouldBe 0
    port443.getAs[Double]("attack_ratio") shouldBe (0.0 +- 0.001)
  }

  test("generateSummary produces valid graph summary statistics") {
    val df = createSyntheticDf()
    val graph = NetworkGraphAnalytics.buildGraph(df)
    val centralityDf = NetworkGraphAnalytics.computePortCentrality(graph, spark)

    val summary = NetworkGraphAnalytics.generateSummary(graph, centralityDf, "synthetic_test_path")

    summary.totalVertices should be > 0L
    summary.totalEdges should be > 0L
    summary.portVerticesCount shouldBe 3
    summary.connectedComponentsCount should be >= 1L
    summary.topTargetedPorts.length shouldBe 3
    summary.topPageRankPorts.length shouldBe 3

    val topPorts = summary.topTargetedPorts.map(_.destinationPort)
    topPorts should contain allOf (80, 443, 22)
    summary.topTargetedPorts.head.inDegree shouldBe 2
  }

  test("saveResults persists Parquet, CSV, and summary JSON successfully") {
    val df = createSyntheticDf()
    val graph = NetworkGraphAnalytics.buildGraph(df)
    val centralityDf = NetworkGraphAnalytics.computePortCentrality(graph, spark)
    val summary = NetworkGraphAnalytics.generateSummary(graph, centralityDf, "synthetic_test_path")

    val tempDir = Files.createTempDirectory("sparkshield-graph-test").toFile
    try {
      NetworkGraphAnalytics.saveResults(centralityDf, summary, tempDir.getPath)

      val parquetDir = new File(tempDir, "parquet")
      val csvDir = new File(tempDir, "csv")
      val summaryFile = new File(tempDir, "graph-analytics-summary.json")

      parquetDir.exists() shouldBe true
      csvDir.exists() shouldBe true
      summaryFile.exists() shouldBe true

      // Verify JSON contains expected fields
      val jsonContent = new String(Files.readAllBytes(summaryFile.toPath))
      jsonContent should include ("total_vertices")
      jsonContent should include ("connected_components_count")
      jsonContent should include ("top_targeted_ports")
      jsonContent should include ("top_pagerank_ports")
    } finally {
      def deleteRec(f: File): Unit = {
        if (f.isDirectory) f.listFiles().foreach(deleteRec)
        f.delete()
      }
      deleteRec(tempDir)
    }
  }
}
