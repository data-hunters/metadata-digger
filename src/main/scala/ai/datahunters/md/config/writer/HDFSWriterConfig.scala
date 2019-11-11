package ai.datahunters.md.config.writer
import ai.datahunters.md.config.ConfigLoader.assignDefaults
import ai.datahunters.md.config.writer.FilesWriterConfig.{Defaults, OutputDirPathKey, OutputFilesNumKey}
import ai.datahunters.md.config.Writer.OutputFormatKey
import ai.datahunters.md.util.FilesHandler
import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession

case class HDFSWriterConfig(val outputDirPath: String,
                            val format: String,
                            val outputFilesNum: Int) extends FilesWriterConfig

object HDFSWriterConfig {

  val StorageName = "hdfs"
  val PathPrefix = "hdfs://"

  def build(config: Config): FilesWriterConfig = {
    val configWithDefaults = assignDefaults(config, Defaults)
    val outputDirPath = configWithDefaults.getString(OutputDirPathKey)
    HDFSWriterConfig(
      FilesHandler.fixPath(PathPrefix, StorageName)(outputDirPath),
      configWithDefaults.getString(OutputFormatKey),
      configWithDefaults.getInt(OutputFilesNumKey)
    )
  }
}