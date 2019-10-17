package ai.datahunters.md.config

import ai.datahunters.md.UnitSpec
import com.typesafe.config.ConfigFactory
import scala.collection.JavaConversions.mapAsJavaMap

class SolrWriterConfigSpec extends UnitSpec {

  "A SolrWriterConfig" should "load default values" in {
    val inputConfig = Map(
      SolrWriterConfig.CollectionKey -> "test_col1",
      SolrWriterConfig.ZKServersKey -> "localhost:2181"
    )
    val config = ConfigFactory.parseMap(inputConfig)
    val outputConfig = SolrWriterConfig.build(config)
    assert(outputConfig.collection === inputConfig.get(SolrWriterConfig.CollectionKey).get)
    assert(outputConfig.zkServers === Seq(inputConfig.get(SolrWriterConfig.ZKServersKey).get))
    assert(outputConfig.zkSolrZNode === None)
  }
}
