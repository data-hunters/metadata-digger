package ai.datahunters.md.writer

import ai.datahunters.md.writer.solr.SolrForeachWriter
import ai.datahunters.md.{SparkBaseSpec, UnitSpec}
import org.apache.solr.client.solrj.impl.CloudSolrClient
import org.apache.spark.sql.DataFrame
import org.mockito.Mockito._
import org.mockito.ArgumentMatchers.any
import org.mockito.InOrder

class SolrWriterSpec extends UnitSpec {

  "A SolrWriter" should "call foreach writer and commit" in {
    val foreachWriter = mock[SolrForeachWriter]
    val solrClient = mock[CloudSolrClient]
    val df = mock[DataFrame]
    val writer = SolrWriter(foreachWriter, solrClient)
    writer.write(df)
    val inOrderExecution: InOrder = org.mockito.Mockito.inOrder(df, solrClient)
    inOrderExecution.verify(df).foreachPartition(foreachWriter)
    inOrderExecution.verify(solrClient).commit()
    inOrderExecution.verify(solrClient).close()
  }

}
