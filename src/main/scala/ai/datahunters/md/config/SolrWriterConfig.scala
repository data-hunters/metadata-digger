package ai.datahunters.md.config

import com.typesafe.config.Config

case class SolrWriterConfig(val collection: String,
                            val zkServers: Seq[String],
                            val zkSolrZNode: Option[String]
                 )

object SolrWriterConfig {

  val CollectionKey = "output.collection"
  val ZKServersKey = "output.zk.servers"
  val ZKSolrZNodeKey = "output.zk.znode"
  val IgnoredTagsKey = "filter.ignoredTags"
  val IgnoredDirectoriesKey = "filter.ignoredDirectories"

  val Defaults = Map(
    IgnoredDirectoriesKey -> "",
    IgnoredTagsKey -> "",
    ZKSolrZNodeKey -> ""
  )
  import ConfigLoader._

  def build(config: Config): SolrWriterConfig = {
    val configWithDefaults = assignDefaults(config, Defaults)
    val znode = configWithDefaults.getString(ZKSolrZNodeKey)
    val znodeOpt = if (znode.isEmpty) None else Some(znode)
    SolrWriterConfig(
      configWithDefaults.getString(CollectionKey),
      configWithDefaults.getString(ZKServersKey).split(ListElementsDelimiter),
      znodeOpt
    )
  }


}