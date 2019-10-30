package ai.datahunters.md.config.writer

import ai.datahunters.md.config.GeneralConfig.NotSupportedStorageException
import ai.datahunters.md.config.{ConfigLoader, GeneralConfig}
import com.typesafe.config.Config

abstract class WriterConfig extends GeneralConfig

object WriterConfig {

  import ai.datahunters.md.config.Writer.StorageNameKey

  val SupportedStorages = Seq(
    LocalFSWriterConfig.StorageName,
    HDFSWriterConfig.StorageName,
    SolrWriterConfig.StorageName
  )

  val Defaults = Map(
    StorageNameKey -> LocalFSWriterConfig.StorageName
  )

  /**
    * Build configuration object specific to the reader.
    *
    * @param config
    * @return
    */
  def build(config: Config): WriterConfig = {
    val configWithDefaults = ConfigLoader.assignDefaults(config, Defaults)
    configWithDefaults.getString(StorageNameKey) match {
      case LocalFSWriterConfig.StorageName => LocalFSWriterConfig.build(configWithDefaults)
      case SolrWriterConfig.StorageName => SolrWriterConfig.build(configWithDefaults)
      case HDFSWriterConfig.StorageName => HDFSWriterConfig.build(configWithDefaults)
      case S3WriterConfig.StorageName => S3WriterConfig.build(configWithDefaults)
      case other => throw new NotSupportedStorageException(s"Not supported storage: $other")
    }

  }
}