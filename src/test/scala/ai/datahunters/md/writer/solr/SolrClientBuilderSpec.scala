package ai.datahunters.md.writer.solr

import java.nio.file.Paths

import ai.datahunters.md.UnitSpec
import ai.datahunters.md.util.SolrClientBuilder

class SolrClientBuilderSpec extends UnitSpec {

  "A SolrClientBuilder" should "create client" in {
    val client = SolrClientBuilder()
      .setZKServers(Seq("localhost:2182,localhost:2185"))
      .setZKSolrChroot(Some("/solr1"))
      .setDefaultCollection("col1")
      .build()
    assert(client.getZkHost === "localhost:2182,localhost:2185/solr1")
    assert(client.getDefaultCollection === "col1")
    assert(client.getIdField === "id")
  }

  it should "create client with no chroot defined" in {
    val client = SolrClientBuilder()
      .setZKServers(Seq("localhost:2183", "localhost:2184"))
      .setZKSolrChroot(None)
      .setDefaultCollection("col2")
      .build()
    assert(client.getZkHost === "localhost:2183,localhost:2184")
    assert(client.getDefaultCollection === "col2")
    assert(client.getIdField === "id")
  }

}
