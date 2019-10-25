package ai.datahunters.md.config

import ai.datahunters.md.config.ConfigLoader.assignDefaults
import ai.datahunters.md.config.GeneralConfig.InvalidConfigException
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
  import S3ReaderConfig._

  override def adjustSparkConfig(sparkSession: SparkSession): Unit = {
    super.adjustSparkConfig(sparkSession)

    val hadoopConf = sparkSession.sparkContext.hadoopConfiguration
    hadoopConf.set(HadoopFsS3ImplKey, HadoopFsS3Impl)
    if (credentialsProvidedInConfig) {
      hadoopConf.set(HadoopS3CredsProviderKey, HadoopS3CredsProvider)
      hadoopConf.set(HadoopS3AccessKeyKey, accessKey)
      hadoopConf.set(HadoopS3SecretKeyKey, secretKey)
      hadoopConf.set(HadoopS3PathStyleAccessKey, HadoopS3PathStyleAccess)
      endpoint.foreach(url => hadoopConf.set(HadoopS3EndpointKey, url))
    }

  }

}

object S3ReaderConfig {

  private val Logger: Logger = LoggerFactory.getLogger(classOf[S3ReaderConfig])

  case class WrongS3PathException(override val msg: String) extends InvalidConfigException(msg)

  /* Spark and Hadoop related property keys */
  val HadoopFsS3ImplKey = "spark.hadoop.fs.s3a.impl"
  val HadoopS3CredsProviderKey = "fs.s3a.aws.credentials.provider"
  val HadoopS3AccessKeyKey = "fs.s3a.access.key"
  val HadoopS3SecretKeyKey = "fs.s3a.secret.key"
  val HadoopS3EndpointKey = "fs.s3a.endpoint"
  val HadoopS3PathStyleAccessKey = "fs.s3a.path.style.access"

  /* Spark and Hadoop related property values */
  val HadoopFsS3Impl = "org.apache.hadoop.fs.s3a.S3AFileSystem"
  val HadoopS3CredsProvider = "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
  val HadoopS3PathStyleAccess = "true"

  /**
    * Name of storage - this value should be used in configuration file to build S3 reader.
    */
  val StorageName = "s3"

  /**
    * Set to true if credentials are provided in configuration file via input.accessKey and input.secretKey.
    * Credentials could be also provided via JCEKS file, in that case, this property should be set to false.
    */
  val CredentialsProvidedInConfigKey = "storage.s3.credsProvided"
  val AccessKeyKey = "storage.s3.accessKey"
  val SecretKeyKey = "storage.s3.secretKey"
  val EndpointKey = "storage.s3.endpoint"

  val PathPrefix = "s3a://"

  val Defaults = Map(
    CredentialsProvidedInConfigKey -> true,
    AccessKeyKey -> "",
    SecretKeyKey -> "",
    EndpointKey -> ""

  )

  def build(config: Config): S3ReaderConfig = {
    val configWithDefaults = assignDefaults(config, Defaults)
    val baseConfig = LocalFSReaderConfig.build(configWithDefaults)
    val endpoint = configWithDefaults.getString(EndpointKey)
    val inputPaths = fixPaths(baseConfig.inputPaths)
    S3ReaderConfig(
      inputPaths,
      baseConfig.partitionsNum,
      configWithDefaults.getBoolean(CredentialsProvidedInConfigKey),
      configWithDefaults.getString(AccessKeyKey),
      configWithDefaults.getString(SecretKeyKey),
      Option(endpoint).filter(_.nonEmpty)
    )
  }

  protected def fixPaths(paths: Seq[String]): Seq[String] = {
    paths.map(p => {
      if (p.startsWith(PathPrefix)) {
        val correctPath = p.replaceFirst(s"^$PathPrefix[/]*", PathPrefix)
        if (!p.contentEquals(correctPath)) Logger.warn(s"Invalid path $p has been changed to: $correctPath")
        correctPath
      } else {
        val pathWithoutLeadingSlashes = p.replaceFirst("^[/]*", "")
        val correctPath = s"$PathPrefix$pathWithoutLeadingSlashes"
        Logger.warn(s"Path has to start with $PathPrefix for S3 Storage, changing path from $p to $correctPath")
        correctPath
      }
    })
  }
}

