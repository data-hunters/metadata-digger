package ai.datahunters.md.config

import ai.datahunters.md.config.BaseConfig.{AllowedDirectoriesKey, AllowedTagsKey, InputPathsKey, OutputDirPathKey, OutputFilesNumKey, OutputFormatKey, PartitionsNumKey}
import ai.datahunters.md.config.LocalModeConfig.{CoresKey, DefaultConfig, MaxMemoryGBKey}
import ai.datahunters.md.config.PropertiesLoader.load


class DistributedModeConfig(override val inputPaths: Seq[String],
                            override val outputDirPath: String,
                            override val format: String,
                            override val partitionsNum: Int,
                            override val outputFilesNum: Int,
                            override val allowedDirectories: Option[Seq[String]],
                            override val allowedTags: Option[Seq[String]])
  extends BaseConfig(inputPaths, outputDirPath, format, partitionsNum, outputFilesNum, allowedDirectories, allowedTags) {

  override def isStandalone(): Boolean = false

}
object DistributedModeConfig {

  import BaseConfig._

  def buildFromProperties(path: String): LocalModeConfig = {
    val props = load(path)
    val config = ConfigLoader.load(path, BaseConfig.DefaultConfig)

    LocalModeConfig(
      config.getString(InputPathsKey).split(ListElementsDelimiter),
      config.getString(OutputDirPathKey),
      config.getString(OutputFormatKey),
      config.getInt(CoresKey),
      config.getInt(MaxMemoryGBKey),
      config.getInt(PartitionsNumKey),
      config.getInt(OutputFilesNumKey),
      config.getString(AllowedDirectoriesKey),
      config.getString(AllowedTagsKey)
    )
  }

}