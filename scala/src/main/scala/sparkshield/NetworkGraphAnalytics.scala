package sparkshield

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory

import java.io.File
import java.time.Instant

// ============================================================
// NetworkGraphAnalytics — GraphX Network Topology & Centrality
//
// Models network flows as a directed bipartite Property Graph between
// client communication profile groups and destination service ports.
//
// SEMANTIC CONTEXT & LIMITATIONS:
// The CICIDS2017 MachineLearningCVE dataset intentionally omits IP
// addresses and source ports to eliminate classifier identity bias.
// To perform topological graph analysis without fabricating fictitious
// IP addresses, this module models the interaction network as:
//   1. Source Vertices (1..1000): Deterministic Client Behavioral Profile
//      Groups, derived by hashing structural TCP flow characteristics
//      (fwd_header_length, init_win_bytes_forward, min_seg_size_forward).
//   2. Destination Vertices (1,000,000 + port): Ground-truth Destination
//      Service Ports (0–65535, e.g. Port 80 HTTP, Port 443 HTTPS).
//   3. Directed Edges: Aggregated communication flows from a behavioral
//      profile group to a destination service port.
//
// Key capabilities:
//   1. Builds a Spark GraphX Property Graph from flow telemetry.
//   2. Calculates In-Degree centrality (number of distinct client
//      profile groups contacting each service port).
//   3. Runs PageRank (10 iterations) to rank structural port centrality.
//   4. Analyzes Connected Components to identify communication clusters.
//   5. Measures attack flow concentration per destination port (e.g. DDoS).
//   6. Persists analytical tables (Parquet, CSV) and summary JSON.
// ============================================================
object NetworkGraphAnalytics {

  private val log = LoggerFactory.getLogger(getClass)

  val PortVertexOffset: Long = 1000000L
  val ClientGroupModulus: Long = 1000L

  case class VertexAttr(
    id: VertexId,
    name: String,
    vertexType: String,
    port: Int
  )

  case class EdgeAttr(
    flowCount: Long,
    attackCount: Long,
    benignCount: Long,
    totalBytes: Double,
    totalPackets: Long
  )

  case class PortRankEntry(
    destinationPort: Int,
    inDegree: Int,
    totalFlows: Long,
    attackFlows: Long,
    attackRatio: Double
  )

  case class PageRankEntry(
    destinationPort: Int,
    pageRankScore: Double,
    totalFlows: Long,
    attackFlows: Long
  )

  case class GraphSummary(
    generatedAt: String,
    inputPath: String,
    totalVertices: Long,
    totalEdges: Long,
    clientVerticesCount: Long,
    portVerticesCount: Long,
    connectedComponentsCount: Long,
    topTargetedPorts: Seq[PortRankEntry],
    topPageRankPorts: Seq[PageRankEntry]
  )

  /**
   * Builds the GraphX Property Graph from a DataFrame of cleaned network flows.
   */
  def buildGraph(df: DataFrame): Graph[VertexAttr, EdgeAttr] = {
    // Filter valid numeric rows and valid port range (0 to 65535)
    val validDf = df.filter(
      col("destination_port").isNotNull &&
      col("destination_port").cast("int") >= 0 &&
      col("destination_port").cast("int") <= 65535 &&
      (!col("has_invalid_numeric") || col("has_invalid_numeric").isNull)
    )

    // Derive deterministic client behavioral profile group ID (source vertex: 1 to 1000)
    // and service port ID (destination vertex: 1,000,000 + port)
    val flowEndpoints = validDf.select(
  (pmod(
    abs(hash(
      coalesce(col("fwd_header_length"), lit(0)),
      coalesce(col("init_win_bytes_forward"), lit(0)),
      coalesce(col("min_seg_size_forward"), lit(0))
    ).cast("long")),
    lit(ClientGroupModulus)
  ) + lit(1L)).as("src_id"),
  (lit(PortVertexOffset) + col("destination_port").cast("long")).as("dst_id"),
  col("destination_port").cast("int").as("port"),
  when(col("label_binary") === "ATTACK", lit(1L)).otherwise(lit(0L)).as("is_attack"),
  when(col("label_binary") === "BENIGN", lit(1L)).otherwise(lit(0L)).as("is_benign"),
  coalesce(col("total_length_of_fwd_packets"), lit(0.0)).cast("double").as("fwd_bytes"),
  coalesce(col("total_length_of_bwd_packets"), lit(0.0)).cast("double").as("bwd_bytes"),
  (coalesce(col("total_fwd_packets"), lit(0.0)) + coalesce(col("total_backward_packets"), lit(0.0))).cast("long").as("packets")
)

    // Distinct client profile group vertices
    val clientVertices: RDD[(VertexId, VertexAttr)] = flowEndpoints
      .select("src_id")
      .distinct()
      .rdd
      .map { row =>
        val id = row.getAs[Long]("src_id")
        (id, VertexAttr(id, s"ProfileGroup-$id", "CLIENT_PROFILE_GROUP", -1))
      }

    // Distinct port vertices
    val portVertices: RDD[(VertexId, VertexAttr)] = flowEndpoints
      .select("dst_id", "port")
      .distinct()
      .rdd
      .map { row =>
        val id = row.getAs[Long]("dst_id")
        val port = row.getAs[Int]("port")
        (id, VertexAttr(id, s"Port-$port", "SERVICE_PORT", port))
      }

    val allVertices: RDD[(VertexId, VertexAttr)] = clientVertices.union(portVertices)

    // Aggregated directed edges between client groups and destination ports
    val aggregatedEdges = flowEndpoints
      .groupBy("src_id", "dst_id")
      .agg(
        count(lit(1)).as("flow_count"),
        sum("is_attack").as("attack_count"),
        sum("is_benign").as("benign_count"),
        sum(col("fwd_bytes") + col("bwd_bytes")).as("total_bytes"),
        sum("packets").as("total_packets")
      )

    val edges: RDD[Edge[EdgeAttr]] = aggregatedEdges.rdd.map { row =>
      val src = row.getAs[Long]("src_id")
      val dst = row.getAs[Long]("dst_id")
      val flowCount = row.getAs[Long]("flow_count")
      val attackCount = row.getAs[Long]("attack_count")
      val benignCount = row.getAs[Long]("benign_count")
      val totalBytes = Option(row.getAs[Double]("total_bytes")).getOrElse(0.0)
      val totalPackets = Option(row.getAs[Long]("total_packets")).getOrElse(0L)
      Edge(src, dst, EdgeAttr(flowCount, attackCount, benignCount, totalBytes, totalPackets))
    }

    val defaultVertex = VertexAttr(0L, "UNKNOWN", "UNKNOWN", -1)
    Graph(allVertices, edges, defaultVertex)
  }

  /**
   * Computes In-Degree, PageRank, and Connected Components on the network graph.
   * Produces a DataFrame analyzing destination service ports.
   */
  def computePortCentrality(graph: Graph[VertexAttr, EdgeAttr], spark: SparkSession): DataFrame = {
    import spark.implicits._

    // Compute In-Degree per vertex
    val inDegrees: RDD[(VertexId, Int)] = graph.inDegrees

    // Run static PageRank (10 iterations)
    val pageRankGraph = graph.staticPageRank(numIter = 10, resetProb = 0.15)
    val pageRanks: RDD[(VertexId, Double)] = pageRankGraph.vertices

    // Aggregate attack and benign flows per destination port from incoming edges
    val edgeStats: RDD[(VertexId, (Long, Long, Long))] = graph.triplets.map { triplet =>
      (triplet.dstId, (triplet.attr.flowCount, triplet.attr.attackCount, triplet.attr.benignCount))
    }.reduceByKey { case ((f1, a1, b1), (f2, a2, b2)) =>
      (f1 + f2, a1 + a2, b1 + b2)
    }

    // Filter to only destination port vertices (id >= PortVertexOffset)
    val portVertices = graph.vertices.filter { case (id, attr) =>
      attr.vertexType == "SERVICE_PORT" && id >= PortVertexOffset
    }

    // Join vertices with in-degree, pagerank, and edge flow stats
    val joined = portVertices
      .leftOuterJoin(inDegrees)
      .leftOuterJoin(pageRanks)
      .leftOuterJoin(edgeStats)
      .map { case (id, (((attr, inDegOpt), prOpt), statsOpt)) =>
        val inDegree = inDegOpt.getOrElse(0)
        val prScore = prOpt.getOrElse(0.0)
        val (flows, attacks, benign) = statsOpt.getOrElse((0L, 0L, 0L))
        val attackRatio = if (flows > 0) attacks.toDouble / flows.toDouble else 0.0
        val port = attr.port
        (port, inDegree, prScore, flows, attacks, benign, attackRatio)
      }

    joined.toDF("destination_port", "in_degree", "pagerank_score", "total_flows", "attack_flows", "benign_flows", "attack_ratio")
      .sort(desc("in_degree"), desc("pagerank_score"))
  }

  /**
   * Generates graph summary metrics and JSON payload.
   */
  def generateSummary(
    graph: Graph[VertexAttr, EdgeAttr],
    centralityDf: DataFrame,
    inputPath: String
  ): GraphSummary = {
    val totalVertices = graph.vertices.count()
    val totalEdges = graph.edges.count()
    val clientCount = graph.vertices.filter(v => v._2.vertexType == "CLIENT_PROFILE_GROUP" || v._2.vertexType == "CLIENT_GROUP").count()
    val portCount = graph.vertices.filter(_._2.vertexType == "SERVICE_PORT").count()

    // Connected components count
    val ccGraph = graph.connectedComponents()
    val connectedComponentsCount = ccGraph.vertices.map(_._2).distinct().count()

    val topTargeted = centralityDf.sort(desc("in_degree")).limit(10).collect().map { r =>
      PortRankEntry(
        destinationPort = r.getAs[Int]("destination_port"),
        inDegree = r.getAs[Int]("in_degree"),
        totalFlows = r.getAs[Long]("total_flows"),
        attackFlows = r.getAs[Long]("attack_flows"),
        attackRatio = math.round(r.getAs[Double]("attack_ratio") * 10000.0) / 100.0
      )
    }.toSeq

    val topPageRank = centralityDf.sort(desc("pagerank_score")).limit(10).collect().map { r =>
      PageRankEntry(
        destinationPort = r.getAs[Int]("destination_port"),
        pageRankScore = math.round(r.getAs[Double]("pagerank_score") * 10000.0) / 10000.0,
        totalFlows = r.getAs[Long]("total_flows"),
        attackFlows = r.getAs[Long]("attack_flows")
      )
    }.toSeq

    GraphSummary(
      generatedAt = Instant.now().toString,
      inputPath = inputPath,
      totalVertices = totalVertices,
      totalEdges = totalEdges,
      clientVerticesCount = clientCount,
      portVerticesCount = portCount,
      connectedComponentsCount = connectedComponentsCount,
      topTargetedPorts = topTargeted,
      topPageRankPorts = topPageRank
    )
  }

  /**
   * Saves centrality tables to Parquet and CSV, and summary metrics to JSON.
   */
  def saveResults(
    centralityDf: DataFrame,
    summary: GraphSummary,
    outputDir: String
  ): Unit = {
    val baseDir = new File(outputDir)
    baseDir.mkdirs()

    val parquetDir = new File(baseDir, "parquet").getPath
    val csvDir = new File(baseDir, "csv").getPath

    log.info(s"Persisting GraphX centrality to: $parquetDir")
    centralityDf.write.mode("overwrite").parquet(parquetDir)

    log.info(s"Persisting GraphX centrality to: $csvDir")
    centralityDf.coalesce(1).write
      .option("header", "true")
      .mode("overwrite")
      .csv(csvDir)

    // Write summary JSON
    val jsonContent = formatSummaryJson(summary)
    val summaryFile = new File(baseDir, "graph-analytics-summary.json")
    val writer = new java.io.PrintWriter(summaryFile, "UTF-8")
    try {
      writer.write(jsonContent)
    } finally {
      writer.close()
    }
    log.info(s"Saved GraphX summary to: ${summaryFile.getPath}")
  }

  private def formatSummaryJson(s: GraphSummary): String = {
    val topTargetedStr = s.topTargetedPorts.map { p =>
      s"""    {
      "destination_port": ${p.destinationPort},
      "in_degree": ${p.inDegree},
      "total_flows": ${p.totalFlows},
      "attack_flows": ${p.attackFlows},
      "attack_ratio": ${p.attackRatio}
    }"""
    }.mkString(",\n")

    val topPrStr = s.topPageRankPorts.map { p =>
      s"""    {
      "destination_port": ${p.destinationPort},
      "pagerank_score": ${p.pageRankScore},
      "total_flows": ${p.totalFlows},
      "attack_flows": ${p.attackFlows}
    }"""
    }.mkString(",\n")

    s"""{
  "generated_at": "${s.generatedAt}",
  "input_path": "${s.inputPath}",
  "total_vertices": ${s.totalVertices},
  "total_edges": ${s.totalEdges},
  "client_vertices_count": ${s.clientVerticesCount},
  "port_vertices_count": ${s.portVerticesCount},
  "connected_components_count": ${s.connectedComponentsCount},
  "top_targeted_ports": [
$topTargetedStr
  ],
  "top_pagerank_ports": [
$topPrStr
  ]
}
"""
  }

  def main(args: Array[String]): Unit = {
    val inputPath = if (args.length > 0) args(0) else "data/processed/cleaned_2026-10-24"
    val outputDir = if (args.length > 1) args(1) else "results/graph-analytics"

    val spark = SparkSessionFactory.create("SparkShield-NetworkGraphAnalytics")

    println("===========================================================================")
    println("  SparkShield — GraphX Network Topology Analytics")
    println(s"  Input Dataset : $inputPath")
    println(s"  Output Path   : $outputDir")
    println("===========================================================================")

    val df = spark.read.parquet(inputPath)

    println(s"Loaded ${df.count()} flow records. Building GraphX Property Graph...")
    val graph = buildGraph(df)

    val centralityDf = computePortCentrality(graph, spark)

    println("\nTop 10 Destination Service Ports by In-Degree Centrality:")
    centralityDf.show(10, truncate = false)

    val summary = generateSummary(graph, centralityDf, inputPath)
    saveResults(centralityDf, summary, outputDir)

    println("\n===========================================================================")
    println("  NetworkGraphAnalytics Execution Complete")
    println(s"  Total Vertices : ${summary.totalVertices} (Clients: ${summary.clientVerticesCount}, Ports: ${summary.portVerticesCount})")
    println(s"  Total Edges    : ${summary.totalEdges}")
    println(s"  Components     : ${summary.connectedComponentsCount}")
    println(s"  Results Saved  : $outputDir/graph-analytics-summary.json")
    println("===========================================================================")
  }
}
