package ai.datahunters.md.config

/**
  * Configuration for Standalone Mode
  * @param inputPaths
  * @param outputDirPath
  * @param format
  * @param cores
  * @param maxMemoryGB
  * @param partitionsNum
  * @param outputFilesNum
  * @param allowedDirectories
  * @param allowedTags
  */
case class LocalModeConfig(override val inputPaths: Seq[String],
                           override val outputDirPath: String,
                           override val format: String,
                           cores: Int,
                           maxMemoryGB: Int,
                           override val partitionsNum: Int,
                           override val outputFilesNum: Int,
                           override val allowedDirectories: Option[Seq[String]],
                           override val allowedTags: Option[Seq[String]])
  extends BaseConfig(inputPaths, outputDirPath, format, partitionsNum, outputFilesNum, allowedDirectories, allowedTags) {

  override def isStandalone(): Boolean = true

}

object LocalModeConfig {

  import BaseConfig._
  import PropertiesLoader._

  val CoresKey = "processing.cores"
  val MaxMemoryGBKey = "processing.maxMemoryGB"

  val DefaultConfig = Map(
    CoresKey -> defaultCores(),
    PartitionsNumKey -> defaultCores() * 2,
    OutputFilesNumKey -> defaultCores() * 2,
    AllowedDirectoriesKey -> All,
    AllowedTagsKey -> All
  )


  def buildFromProperties(path: String): LocalModeConfig = {
    val props = load(path)
    val config = ConfigLoader.load(path, DefaultConfig)

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


  private def defaultCores(): Int = Math.max(1, Runtime.getRuntime.availableProcessors - 1)


}