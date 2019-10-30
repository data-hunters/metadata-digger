package ai.datahunters.md.config.reader

import ai.datahunters.md.config.ConfigLoader
import ai.datahunters.md.util.FilesHandler
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

  val StorageName = "file"
  val PathPrefix = "file://"

  def build(config: Config): FilesReaderConfig = {
    val configWithDefaults = ConfigLoader.assignDefaults(config, Defaults)
    val rawInputPaths = getInputPaths(configWithDefaults)
    val inputPaths = FilesHandler.fixPaths(PathPrefix, StorageName)(rawInputPaths)
    LocalFSReaderConfig(
      inputPaths,
      getPartitionsNum(configWithDefaults)
    )
  }

}

