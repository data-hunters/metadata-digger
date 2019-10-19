package ai.datahunters.md.config

import com.typesafe.config.Config

case class FilesReaderConfig(val inputPaths: Seq[String],
                             val partitionsNum: Int) extends ReaderConfig {

}

object FilesReaderConfig {
  val InputPathsKey = "input.paths"
  val PartitionsNumKey = "input.partitions"

  import ConfigLoader._

  val Defaults = Map(
    PartitionsNumKey -> -1
  )

  def build(config: Config): FilesReaderConfig = {
    val configWithDefaults = assignDefaults(config, Defaults)
    FilesReaderConfig(
      configWithDefaults.getString(InputPathsKey).split(ListElementsDelimiter),
      configWithDefaults.getInt(PartitionsNumKey)
    )
  }
}