package ai.datahunters.md.config

import org.apache.spark.sql.SparkSession

trait GeneralConfig {
  /**
    * Applies all necessary config adjustments related to Spark and Hadoop.
    * Classes extending GeneralConfig should set all properties specific to the particular module they relate to.
    *
    * @param sparkSession
    */
  def adjustSparkConfig(sparkSession: SparkSession)
}

object GeneralConfig {

  case class NotSupportedStorageException(msg: String) extends RuntimeException

  val SparkDFPartitionsNumKey = "spark.sql.shuffle.partitions"
}
