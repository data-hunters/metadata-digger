package ai.datahunters.md.config

object GeneralConfig {

  class InvalidConfigException(val msg: String) extends RuntimeException

  val SparkDFPartitionsNumKey = "spark.sql.shuffle.partitions"
}
