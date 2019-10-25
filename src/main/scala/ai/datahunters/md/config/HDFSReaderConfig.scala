package ai.datahunters.md.config

import ai.datahunters.md.config.ConfigLoader.assignDefaults
import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession

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
                            partitionsNum: Int,
                            kerberosEnabled: Boolean,
                            username: Option[String],
                            keytabPath: Option[String],
                            krb5confPath: String) extends FilesReaderConfig {
  override def adjustSparkConfig(sparkSession: SparkSession): Unit = ???
}

object HDFSReaderConfig {

  val DefaultKerberosConfPath = "/etc/krb5.conf"

  val KerberosEnabledKey = "input.hdfs.kerberosEnabled"
  val KerberosUserKey = "input.hdfs.user"
  val KerberosKeytabPathKey = "input.hdfs.keytabPath"
  val KerberosConfPathKey = "input.hdfs.krb5ConfPath"

  val Defaults = Map(
    KerberosEnabledKey -> false,
    KerberosConfPathKey -> DefaultKerberosConfPath,
    KerberosUserKey -> "",
    KerberosKeytabPathKey -> ""
  )

  def build(config: Config): HDFSReaderConfig = {
    val configWithDefaults = assignDefaults(config, Defaults)
    val baseConfig = LocalFSReaderConfig.build(configWithDefaults)
    val username = configWithDefaults.getString(KerberosUserKey)
    val keytabPath = configWithDefaults.getString(KerberosKeytabPathKey)

    HDFSReaderConfig(
      baseConfig.inputPaths,
      baseConfig.partitionsNum,
      configWithDefaults.getBoolean(KerberosEnabledKey),
      Option(username).filter(_.nonEmpty),
      Option(keytabPath).filter(_.nonEmpty),
      configWithDefaults.getString(KerberosConfPathKey)
    )
  }
}
