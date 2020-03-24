package ai.datahunters.md.writer.solr

import java.util
import java.util.Optional

import ai.datahunters.md.config.KrbConfig
import ai.datahunters.md.security.JaasConfigurationFactory
import ai.datahunters.md.util.FilesHandler
import javax.security.auth.login.Configuration
import org.apache.solr.client.solrj.impl.{CloudSolrClient, HttpClientUtil, Krb5HttpClientBuilder}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters

case class SolrClientBuilder() {
  import JavaConverters._
  import SolrClientBuilder._

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

  def setJaas(conf: KrbConfig): SolrClientBuilder = {
    System.setProperty(JaasSecurityProperty, JaasConfPathPlaceholder)
    val finalKP = FilesHandler.sparkFilePath(conf.userKeytabFileName)
    Configuration.setConfiguration(JaasConfigurationFactory.create(finalKP, conf.principal, conf.debug))
    Logger.info("Building KRB5 client for solr")
    val krbBuild = new Krb5HttpClientBuilder()
    val kb = krbBuild.getBuilder()
    HttpClientUtil.setHttpClientBuilder(kb)
    this
  }

  def build(): CloudSolrClient = {
    val client = new CloudSolrClient.Builder(zkServers, zkSolrChroot).build()
    defaultCollection.foreach(client.setDefaultCollection)
    client
  }

}

object SolrClientBuilder {

  val Logger = LoggerFactory.getLogger(classOf[SolrClientBuilder])
  val JaasSecurityProperty = "java.security.auth.login.config"
  val JaasConfPathPlaceholder = "embedded"

}