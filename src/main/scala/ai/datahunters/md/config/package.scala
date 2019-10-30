package ai.datahunters.md

import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

package object config {

  object Writer {

    val OutputFormatKey = "output.format"
    val StorageNameKey = "output.storage.name"
  }

  object S3 {

    private val Logger: Logger = LoggerFactory.getLogger(S3.getClass)

    val StorageName = "s3"

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
      * Set to true if credentials are provided in configuration file via input.accessKey and input.secretKey.
      * Credentials could be also provided via JCEKS file, in that case, this property should be set to false.
      */
    val CredentialsProvidedInConfigKey = "storage.s3.credsProvided"
    val AccessKeyKey = "storage.s3.accessKey"
    val SecretKeyKey = "storage.s3.secretKey"
    val EndpointKey = "storage.s3.endpoint"

    val PathPrefix = "s3a://"

    def adjustSparkConfig(sparkSession: SparkSession,
                            credentialsProvidedInConfig: Boolean,
                            accessKey: String,
                            secretKey: String,
                            endpoint: Option[String]): Unit = {

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

    /**
      * Adjust path by forcing usage of prefix specified in PathPrefix
      * @param path
      * @return
      */
    def fixPath(path: String): String = {
      if (path.startsWith(PathPrefix)) {
        val correctPath = path.replaceFirst(s"^$PathPrefix[/]*", PathPrefix)
        if (!path.contentEquals(correctPath)) Logger.warn(s"Invalid path $path has been changed to: $correctPath")
        correctPath
      } else {
        val pathWithoutLeadingSlashes = path.replaceFirst("^[/]*", "")
        val correctPath = s"$PathPrefix$pathWithoutLeadingSlashes"
        Logger.warn(s"Path has to start with $PathPrefix for S3 Storage, changing path from $path to $correctPath")
        correctPath
      }
    }

    /**
      * Adjust paths by forcing usage of prefix specified in PathPrefix
      *
      * @param paths
      * @return
      */
    def fixPaths(paths: Seq[String]): Seq[String] = {
      paths.map(fixPath)
    }
  }
}
