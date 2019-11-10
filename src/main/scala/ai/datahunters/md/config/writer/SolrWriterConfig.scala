package ai.datahunters.md.config.writer

import ai.datahunters.md.config.ConfigLoader
import ai.datahunters.md.util.TextUtils
import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession

case class SolrWriterConfig(dateTimeTags: Seq[String],
                            integerTags: Seq[String],
                            collection: String,
                            zkServers: Seq[String],
                            zkSolrZNode: Option[String]) extends WriterConfig {

  import SolrWriterConfig._


  override def adjustSparkConfig(sparkSession: SparkSession): Unit = {
  }
}

object SolrWriterConfig {

  val StorageName = "solr"

  case class TooManyGeoTagsException(msg: String) extends RuntimeException(msg)


  val IntegerTagsKey = "output.solr.conversion.integerTags"
  val DateTimeTagsKeys = "output.solr.conversion.dateTimeTags"
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
    IntegerTagsKey -> ""
  )
  import ConfigLoader._

  def build(config: Config): SolrWriterConfig = {
    val configWithDefaults = assignDefaults(config, Defaults)
    val znode = configWithDefaults.getString(ZKSolrZNodeKey)
    val znodeOpt = if (znode.isEmpty) None else Some(znode)
    SolrWriterConfig(
      configWithDefaults.getString(DateTimeTagsKeys).split(ListElementsDelimiter),
      configWithDefaults.getString(IntegerTagsKey).split(ListElementsDelimiter),
      configWithDefaults.getString(CollectionKey),
      configWithDefaults.getString(ZKServersKey).split(ListElementsDelimiter),
      znodeOpt
    )
  }

}