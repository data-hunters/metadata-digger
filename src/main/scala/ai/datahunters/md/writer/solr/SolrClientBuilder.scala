package ai.datahunters.md.writer.solr

import java.util
import java.util.Optional

import org.apache.solr.client.solrj.impl.CloudSolrClient

import scala.collection.JavaConverters

case class SolrClientBuilder() {
  import JavaConverters._

  private var zkSolrChroot: Optional[String] = Optional.empty()
  private var zkServers: java.util.List[String] = new util.ArrayList[String]()
  private var defaultCollection: Option[String] = None

  def setZKServers(servers: Seq[String]): SolrClientBuilder = {
    zkServers = servers.asJava
    this
  }

  def setZKSolrChroot(chroot: Option[String]): SolrClientBuilder = {
    zkSolrChroot = chroot.map(c => Optional.of(c)).getOrElse(Optional.empty())
    this
  }

  def setDefaultCollection(collectionName: String): SolrClientBuilder = {
    defaultCollection = Some(collectionName)
    this
  }

  def build(): CloudSolrClient = {
    val client = new CloudSolrClient.Builder(zkServers, zkSolrChroot).build()
    defaultCollection.foreach(client.setDefaultCollection)
    client
  }

}
