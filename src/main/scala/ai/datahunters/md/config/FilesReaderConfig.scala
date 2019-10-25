package ai.datahunters.md.config

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

  val StorageName = "file"
  val InputPathsKey = "input.paths"
  val PartitionsNumKey = "input.partitions"

  val Defaults = Map(
    PartitionsNumKey -> -1
  )


}