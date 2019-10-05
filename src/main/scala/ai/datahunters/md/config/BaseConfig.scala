package ai.datahunters.md.config

import ai.datahunters.md.config.LocalModeConfig.defaultCores

abstract class BaseConfig(
  val inputPaths: Seq[String],
  val outputDirPath: String,
  val format: String,
  val partitionsNum: Int,
  val outputFilesNum: Int,
  val allowedDirectories: Option[Seq[String]],
  val allowedTags: Option[Seq[String]]) {

  def isStandalone(): Boolean
}

object BaseConfig {

  val InputPathsKey = "input.paths"
  val OutputDirPathKey = "output.directoryPath"
  val OutputFormatKey = "output.format"
  val PartitionsNumKey = "processing.partitions"
  val OutputFilesNumKey = "output.filesNumber"
  val AllowedDirectoriesKey = "filter.allowedMetadataDirectories"
  val AllowedTagsKey = "filter.allowedTags"

  val All = "*"
  val ListElementsDelimiter = ","

  val DefaultConfig = Map(
    AllowedDirectoriesKey -> All,
    AllowedTagsKey -> All
  )

  implicit def strToSeq(configVal: String): Option[Seq[String]] = {
    if (configVal.equalsIgnoreCase(All)) {
      None
    } else {
      Some(
        configVal.split(ListElementsDelimiter).map(_.trim).toSeq
      )
    }
  }

}