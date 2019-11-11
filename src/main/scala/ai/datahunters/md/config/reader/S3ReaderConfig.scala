package ai.datahunters.md.config.reader

import ai.datahunters.md.config.{ConfigLoader, S3}
import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

/**
  * Configuration for S3 reader.
  *
  * @param inputPaths
  * @param partitionsNum
  * @param credentialsProvidedInConfig Set to true if credentials are provided in configuration file via input.accessKey and input.secretKey.
  *                                    Credentials could be also provided via JCEKS file, in that case, this property should be set to false.
  * @param accessKey
  * @param secretKey
  */
case class S3ReaderConfig(inputPaths: Seq[String],
                          partitionsNum: Int,
                          credentialsProvidedInConfig: Boolean,
                          accessKey: String,
                          secretKey: String,
                          endpoint: Option[String]) extends FilesReaderConfig {

  override def adjustSparkConfig(sparkSession: SparkSession): Unit = {
    super.adjustSparkConfig(sparkSession)
    S3.adjustSparkConfig(sparkSession, credentialsProvidedInConfig, accessKey, secretKey, endpoint)
  }

}

object S3ReaderConfig {

  import ai.datahunters.md.config.S3._

  private val Logger: Logger = LoggerFactory.getLogger(classOf[S3ReaderConfig])

  /**
    * Name of storage - this value should be used in configuration file to build S3 reader.
    */
  val StorageName = S3.StorageName

  val Defaults = FilesReaderConfig.Defaults ++ Map(
    CredentialsProvidedInConfigKey -> true,
    AccessKeyKey -> "",
    SecretKeyKey -> "",
    EndpointKey -> ""
  )

  def build(config: Config): S3ReaderConfig = {
    val configWithDefaults = ConfigLoader.assignDefaults(config, Defaults)
    val rawInputPaths = FilesReaderConfig.getInputPaths(configWithDefaults)
    val endpoint = configWithDefaults.getString(EndpointKey)
    val inputPaths = fixPaths(rawInputPaths)
    S3ReaderConfig(
      inputPaths,
      FilesReaderConfig.getPartitionsNum(configWithDefaults),
      configWithDefaults.getBoolean(CredentialsProvidedInConfigKey),
      configWithDefaults.getString(AccessKeyKey),
      configWithDefaults.getString(SecretKeyKey),
      Option(endpoint).filter(_.nonEmpty)
    )
  }


}

