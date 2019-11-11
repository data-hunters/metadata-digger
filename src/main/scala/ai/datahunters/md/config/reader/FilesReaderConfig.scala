package ai.datahunters.md.config.reader

import ai.datahunters.md.config.ConfigLoader.ListElementsDelimiter
import ai.datahunters.md.config.GeneralConfig
import ai.datahunters.md.util.TextUtils
import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession

trait FilesReaderConfig extends ReaderConfig {

  def inputPaths: Seq[String]
  def partitionsNum: Int

  override def adjustSparkConfig(sparkSession: SparkSession): Unit = {
    if (partitionsNum > 0) sparkSession.conf.set(GeneralConfig.SparkDFPartitionsNumKey, partitionsNum)
  }

}

object FilesReaderConfig {


  val InputPathsKey = "input.paths"
  val PartitionsNumKey = "input.partitions"

  val Defaults = Map(
    PartitionsNumKey -> -1
  )

  def getInputPaths(config: Config): Seq[String] = {
    config.getString(InputPathsKey).split(ListElementsDelimiter).map(_.trim)
  }

  def getPartitionsNum(config: Config): Int = {
    config.getInt(PartitionsNumKey)
  }

}