package ai.datahunters.md.config.writer

import ai.datahunters.md.config.ConfigLoader
import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession

trait FilesWriterConfig extends WriterConfig {

  def outputDirPath: String
  def format: String
  def outputFilesNum: Int

  override def adjustSparkConfig(sparkSession: SparkSession): Unit = {}
}

object FilesWriterConfig {

  import ConfigLoader.assignDefaults
  import WriterConfig._

  val OutputDirPathKey = "output.directoryPath"
  val OutputFilesNumKey = "output.filesNumber"

  val Defaults = Map(
    OutputFilesNumKey -> 1
  )


}