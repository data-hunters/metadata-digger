package ai.datahunters.md.config.reader

import ai.datahunters.md.config.ConfigLoader
import ai.datahunters.md.util.FilesHandler
import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

/**
  * Configuration for HDFS reader.
  * @param inputPaths
  * @param partitionsNum
  * @param kerberosEnabled
  * @param username
  * @param keytabPath
  * @param krb5confPath
  */
case class HDFSReaderConfig(inputPaths: Seq[String],
                            partitionsNum: Int) extends FilesReaderConfig {
  override def adjustSparkConfig(sparkSession: SparkSession): Unit = {

  }
}

object HDFSReaderConfig {

  val Logger = LoggerFactory.getLogger(classOf[HDFSReaderConfig])

  val StorageName = "hdfs"
  val DefaultKerberosConfPath = "/etc/krb5.conf"

  val KerberosEnabledKey = "input.hdfs.kerberosEnabled"
  val KerberosUserKey = "input.hdfs.user"
  val KerberosKeytabPathKey = "input.hdfs.keytabPath"
  val KerberosConfPathKey = "input.hdfs.krb5ConfPath"

  val PathPrefix = "hdfs://"

  val Defaults = FilesReaderConfig.Defaults ++ Map(
    KerberosEnabledKey -> false,
    KerberosConfPathKey -> DefaultKerberosConfPath,
    KerberosUserKey -> "",
    KerberosKeytabPathKey -> ""
  )

  def build(config: Config): HDFSReaderConfig = {
    val configWithDefaults = ConfigLoader.assignDefaults(config, Defaults)
    val rawInputPaths = FilesReaderConfig.getInputPaths(configWithDefaults)
    val inputPaths = FilesHandler.fixPaths(PathPrefix, StorageName)(rawInputPaths)
    HDFSReaderConfig(
      inputPaths,
      FilesReaderConfig.getPartitionsNum(configWithDefaults)
    )
  }


}
