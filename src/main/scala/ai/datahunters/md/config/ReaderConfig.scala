package ai.datahunters.md.config

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.SparkSession

/**
  * Base class for all config classes related to readers.
  */
abstract class ReaderConfig {

  /**
    * Applies all necessary config adjustments related to Spark and Hadoop.
    * All classes extending ReaderConfig should set all config specific to the reader.
    *
    * @param sparkSession
    */
  def adjustSparkConfig(sparkSession: SparkSession)

}

object ReaderConfig {

  case class NotSupportedStorageException(msg: String) extends RuntimeException

  val StorageNameKey = "input.storage.name"
  val SupportedStorages = Seq(
    LocalFSReaderConfig.StorageName,
    HDFSReaderConfig.StorageName,
    S3ReaderConfig.StorageName
  )

  val Defaults = Map(
    StorageNameKey -> LocalFSReaderConfig.StorageName
  )

  /**
    * Build configuration object specific to the reader.
    *
    * @param config
    * @return
    */
  def build(config: Config): ReaderConfig = {
    val configWithDefaults = ConfigLoader.assignDefaults(config, Defaults)
    configWithDefaults.getString(StorageNameKey) match {
      case LocalFSReaderConfig.StorageName => LocalFSReaderConfig.build(configWithDefaults)
      case S3ReaderConfig.StorageName => S3ReaderConfig.build(configWithDefaults)
      case HDFSReaderConfig.StorageName => HDFSReaderConfig.build(configWithDefaults)
      case other => throw new NotSupportedStorageException(s"Not supported storage: $other")
    }

  }
}