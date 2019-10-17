package ai.datahunters.md.config

import com.typesafe.config.Config

case class FilesWriterConfig(val outputDirPath: String,
                             val format: String,
                             val outputFilesNum: Int) extends WriterConfig {

}

object FilesWriterConfig {

  import ConfigLoader.assignDefaults
  import WriterConfig._

  val OutputDirPathKey = "output.directoryPath"
  val OutputFilesNumKey = "output.filesNumber"

  val Defaults = Map(
    OutputFilesNumKey -> 1
  )

  def build(config: Config): FilesWriterConfig = {
    val configWithDefaults = assignDefaults(config, Defaults)
    FilesWriterConfig(
      configWithDefaults.getString(OutputDirPathKey),
      configWithDefaults.getString(OutputFormatKey),
      configWithDefaults.getInt(OutputFilesNumKey)
    )
  }
}