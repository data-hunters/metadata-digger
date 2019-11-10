package ai.datahunters.md.config.reader

import ai.datahunters.md.config.GeneralConfig.NotSupportedStorageException
import ai.datahunters.md.config._
import com.typesafe.config.Config

/**
  * Base class for all config classes related to readers.
  */
abstract class ReaderConfig extends GeneralConfig

object ReaderConfig {


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
  def apply(config: Config): ReaderConfig = {
    val configWithDefaults = ConfigLoader.assignDefaults(config, Defaults)
    configWithDefaults.getString(StorageNameKey) match {
      case LocalFSReaderConfig.StorageName => LocalFSReaderConfig.build(configWithDefaults)
      case S3ReaderConfig.StorageName => S3ReaderConfig.build(configWithDefaults)
      case HDFSReaderConfig.StorageName => HDFSReaderConfig.build(configWithDefaults)
      case other => throw new NotSupportedStorageException(s"Not supported storage: $other")
    }

  }
}