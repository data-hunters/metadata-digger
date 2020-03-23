package ai.datahunters.md.config.writer

import java.nio.file.Paths

import ai.datahunters.md.config.{ConfigLoader, KrbConfig}
import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

case class SolrWriterConfig(dateTimeTags: Seq[String],
                            integerTags: Seq[String],
                            collection: String,
                            zkServers: Seq[String],
                            zkSolrZNode: Option[String],
                            krbConfig: Option[KrbConfig]) extends WriterConfig {

  import SolrWriterConfig._


  override def adjustSparkConfig(sparkSession: SparkSession): Unit = {
    krbConfig.foreach(p => {
      Logger.info(s"Adding keytab ${p.keytabPath}")
      sparkSession.sparkContext.addFile(p.keytabPath)
    })
  }
}

object SolrWriterConfig {

  val StorageName = "solr"

  val Logger = LoggerFactory.getLogger(classOf[SolrWriterConfig])

  case class TooManyGeoTagsException(msg: String) extends RuntimeException(msg)

  val OutputSolrPrefix = "output.solr"
  val IntegerTagsKey = s"${OutputSolrPrefix}.conversion.integerTags"
  val DateTimeTagsKeys = s"${OutputSolrPrefix}.conversion.dateTimeTags"
  val JaasConfPathKey = s"${OutputSolrPrefix}.security.jaasPath"
  val UserKeytabPathKey = s"${OutputSolrPrefix}.security.keytabPath"
  val CollectionKey = "output.collection"
  val ZKServersKey = "output.zk.servers"
  val ZKSolrZNodeKey = "output.zk.znode"
  val IgnoredTagsKey = "filter.ignoredTags"
  val IgnoredDirectoriesKey = "filter.ignoredDirectories"

  val Defaults = Map(
    IgnoredDirectoriesKey -> "",
    IgnoredTagsKey -> "",
    ZKSolrZNodeKey -> "",
    DateTimeTagsKeys -> "",
    IntegerTagsKey -> "",
    UserKeytabPathKey -> "",
    JaasConfPathKey -> ""
  )
  import ConfigLoader._

  def build(config: Config): SolrWriterConfig = {
    val configWithDefaults = assignDefaults(config, Defaults)
    val znode = configWithDefaults.getString(ZKSolrZNodeKey)
    val jaasConfPath = configWithDefaults.getString(JaasConfPathKey)
    val keytabPath = configWithDefaults.getString(UserKeytabPathKey)
    val znodeOpt = if (znode.isEmpty) None else Some(znode)
    val krbConf = KrbConfig.loadKrbConfig(configWithDefaults, OutputSolrPrefix)
    SolrWriterConfig(
      configWithDefaults.getString(DateTimeTagsKeys).split(ListElementsDelimiter),
      configWithDefaults.getString(IntegerTagsKey).split(ListElementsDelimiter),
      configWithDefaults.getString(CollectionKey),
      configWithDefaults.getString(ZKServersKey).split(ListElementsDelimiter),
      znodeOpt,
      krbConf
    )
  }



}