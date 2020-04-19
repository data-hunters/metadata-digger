package ai.datahunters.md.writer
import ai.datahunters.md.writer.solr.SolrForeachWriter
import org.apache.solr.client.solrj.impl.CloudSolrClient
import org.apache.spark.sql.{DataFrame, functions}
import org.slf4j.LoggerFactory

/**
  * Write DataFrame to Solr using foreachPartition methods which execute write operations in distributed flow.
  *
  * @param foreachWriter
  * @param client Solr client used for triggering commit in Solr after completion of all writing tasks
  */
case class SolrWriter(foreachWriter: SolrForeachWriter, client: CloudSolrClient) extends PipelineSink {

  val logger = LoggerFactory.getLogger(classOf[SolrWriter])

  override def write(outputDF: DataFrame): Unit = {
    logger.info("Solr Writer initialization.")
    outputDF.foreachPartition(foreachWriter)
    logger.info("All documents have been added, triggering commit.")
    client.commit()
    logger.info("Done.")
    client.close()
  }

}

object SolrWriter {

  val FormatName = "solr"

}
