package ai.datahunters.md.writer.solr

import java.util
import java.util.{Date, Optional}

import ai.datahunters.md.config.writer.SolrWriterConfig
import ai.datahunters.md.schema.SchemaConfig
import ai.datahunters.md.util.{DateTimeUtils, SolrClientBuilder, TextUtils}
import org.apache.solr.client.solrj.impl.CloudSolrClient
import org.apache.solr.common.SolrInputDocument
import org.apache.spark.api.java.function.ForeachPartitionFunction
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{ArrayType, StringType, StructField}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters
import scala.collection.mutable.ArrayBuffer

/**
  * Convert all Row objects (in partition) to SolrInputDocument and index to Solr.
  *
  * @param putBatchSize
  */
case class SolrForeachWriter(config: SolrWriterConfig,
                             private val strToIntHandler: (String, SolrInputDocument) => (String) => Unit = Converters.addInt,
                             private val strToDateTimeHandler: (String, SolrInputDocument) => (String) => Unit = Converters.addDatetime,
                             private val putBatchSize: Int = 1000) extends ForeachPartitionFunction[Row] {


  private val logger = LoggerFactory.getLogger(classOf[SolrForeachWriter])

  override def call(partitionRows: util.Iterator[Row]): Unit = {
    val client = SolrClientBuilder().setZKServers(config.zkServers)
        .setZKSolrChroot(config.zkSolrZNode)
        .setDefaultCollection(config.collection)
        .build()
    val docsBuffer = ArrayBuffer[SolrInputDocument]()
    processAll(partitionRows, client)
    client.close()
  }

  protected def processAll(partitionRows: util.Iterator[Row], client: CloudSolrClient): Unit = {
    val docs = ArrayBuffer[Row]()
    while (partitionRows.hasNext) {
      val r = partitionRows.next()
      docs.append(r)
    }
    docs.grouped(putBatchSize)
      .foreach(processBatch(client))
  }

  protected def processBatch(client: CloudSolrClient)(batch: Seq[Row]): Unit = {
    import JavaConverters._
    val docs = batch.map(rowToDoc)
      .asJava
    client.add(docs)
  }

  protected def rowToDoc(row: Row): SolrInputDocument = {
    val doc = new SolrInputDocument()
    val fields = row.schema.fields
    logger.debug(s"Output fields for Solr: ${fields.mkString(", ")}")
    fields.foreach(f => {
      val name = f.name
      if (!checkIfEmpty(row, f)) {
        if (f.dataType.isInstanceOf[ArrayType]) {
          val list: Seq[Any] = row.getAs(name)
          list.foreach(el => {
            doc.addField(name, el)
          })
        } else {
          if (config.dateTimeTags.contains(name)) {
            strToDateTimeHandler(name, doc)(row.getAs(name))
          } else if (config.integerTags.contains(name)) {
            strToIntHandler(name, doc)(row.getAs(name))
          } else {
            doc.addField(name, row.getAs[Any](name))
          }
        }
      }
    })
    doc.addField(FixedFields.ProcessingDateTimeField, new Date())
    doc
  }

  private def checkIfEmpty(row: Row, f: StructField): Boolean = {
    val name = f.name
    return if (f.dataType == StringType) {
      val strV = row.getAs[String](name)
      (TextUtils.isEmpty(strV))
    } else {
      row.isNullAt(row.schema.fieldIndex(name))
    }
  }
}

