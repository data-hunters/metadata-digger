package ai.datahunters.md.config.writer
import ai.datahunters.md.config.ConfigLoader.assignDefaults
import ai.datahunters.md.config.S3
import ai.datahunters.md.config.S3._
import ai.datahunters.md.config.reader.FilesReaderConfig
import ai.datahunters.md.config.writer.FilesWriterConfig.{OutputDirPathKey, OutputFilesNumKey}
import ai.datahunters.md.config.Writer.OutputFormatKey
import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession

case class S3WriterConfig(val outputDirPath: String,
                          val format: String,
                          val outputFilesNum: Int,
                          credentialsProvidedInConfig: Boolean,
                          accessKey: String,
                          secretKey: String,
                          endpoint: Option[String]) extends FilesWriterConfig {

  override def adjustSparkConfig(sparkSession: SparkSession): Unit = {
    S3.adjustSparkConfig(sparkSession, credentialsProvidedInConfig, accessKey, secretKey, endpoint)
  }
}

object S3WriterConfig {

  /**
    * Name of storage - this value should be used in configuration file to build S3 Writer.
    */
  val StorageName = S3.StorageName

  val Defaults = FilesWriterConfig.Defaults ++ Map(
    CredentialsProvidedInConfigKey -> true,
    AccessKeyKey -> "",
    SecretKeyKey -> "",
    EndpointKey -> ""
  )

  def build(config: Config): S3WriterConfig = {
    val configWithDefaults = assignDefaults(config, Defaults)
    val outputDirPath = configWithDefaults.getString(OutputDirPathKey)
    val endpoint = configWithDefaults.getString(EndpointKey)
    S3WriterConfig(
      fixPath(outputDirPath),
      configWithDefaults.getString(OutputFormatKey),
      configWithDefaults.getInt(OutputFilesNumKey),
      configWithDefaults.getBoolean(CredentialsProvidedInConfigKey),
      configWithDefaults.getString(AccessKeyKey),
      configWithDefaults.getString(SecretKeyKey),
      Option(endpoint).filter(_.nonEmpty)
    )
  }

}
