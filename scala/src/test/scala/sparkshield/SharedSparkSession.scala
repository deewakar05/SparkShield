package sparkshield

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.io.File

/**
 * Shared base trait for SparkShield ScalaTest suites.
 * Centralizes local SparkSession lifecycle management and filesystem cleanup helpers.
 * Eliminates duplicate session initialization and recursive directory deletion boilerplate.
 */
trait SharedSparkSession extends AnyFunSuite with BeforeAndAfterAll {

  @transient var spark: SparkSession = _

  def testMaster: String = "local[2]"
  def testShufflePartitions: String = "2"
  def appName: String = getClass.getSimpleName

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName(appName)
      .master(testMaster)
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", testShufflePartitions)
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
      spark = null
    }
  }

  /**
   * Safely deletes a file or directory recursively.
   * Handles nulls and missing paths gracefully without throwing NullPointerExceptions.
   */
  def deleteRecursively(f: File): Unit = {
    if (f != null && f.exists()) {
      if (f.isDirectory) {
        val children = f.listFiles()
        if (children != null) {
          children.foreach(deleteRecursively)
        }
      }
      f.delete()
    }
  }
}
