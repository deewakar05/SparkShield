package sparkshield

import org.apache.spark.sql.SparkSession

/**
 * SparkSession factory used by all SparkShield modules.
 *
 * Centralises SparkSession configuration so that individual modules
 * do not hardcode master, memory, or UI settings.
 */
object SparkSessionFactory {

  /**
   * Create (or retrieve) a SparkSession.
   *
   * @param appName  Application name shown in Spark UI / logs
   * @param master   Spark master URL (default: local[*])
   * @param extras   Additional Spark configuration key-value pairs
   */
  def create(
    appName : String,
    master  : String = "local[*]",
    extras  : Map[String, String] = Map.empty
  ): SparkSession = {

    val builder = SparkSession
      .builder()
      .appName(appName)
      .master(master)
      .config("spark.ui.enabled", "false")
      .config("spark.driver.memory", "2g")
      .config("spark.sql.shuffle.partitions", "8") // lower than default 200 for local mode

    extras.foreach { case (k, v) => builder.config(k, v) }

    val spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark
  }
}
