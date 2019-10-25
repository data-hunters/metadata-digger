package ai.datahunters.md.config

import ai.datahunters.md.config.ConfigLoader.ListElementsDelimiter
import com.typesafe.config.Config

/**
  * Configuration for Local File System reader.
  *
  * @param inputPaths
  * @param partitionsNum
  */
case class LocalFSReaderConfig(inputPaths: Seq[String], partitionsNum: Int) extends FilesReaderConfig

object LocalFSReaderConfig {

  import FilesReaderConfig._

  def build(config: Config): FilesReaderConfig = {
    val configWithDefaults = ConfigLoader.assignDefaults(config, Defaults)
    LocalFSReaderConfig(
      configWithDefaults.getString(InputPathsKey).split(ListElementsDelimiter).map(_.trim),
      configWithDefaults.getInt(PartitionsNumKey)
    )
  }

}

