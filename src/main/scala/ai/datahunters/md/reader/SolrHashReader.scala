package ai.datahunters.md.reader

import java.util

import ai.datahunters.md.config.writer.SolrWriterConfig
import ai.datahunters.md.processor.ColumnNamesConverterFactory
import ai.datahunters.md.util.SolrClientBuilder
import org.apache.solr.client.solrj.impl.CloudSolrClient
import org.apache.solr.common.params.MapSolrParams
import org.apache.solr.common.{SolrDocument, SolrDocumentList}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.JavaConversions._

/**
  * Read  id and hashes to DataFrame mainly for processing reduction
  *
  * @param sparkSession
  * @param config
  */
case class SolrHashReader(sparkSession: SparkSession,
                          config: SolrWriterConfig,
                          namingConvention: String) extends PipelineSource {

  import ai.datahunters.md.processor.HashExtractor._
  import ai.datahunters.md.reader.SolrHashReader._
  import ai.datahunters.md.util.HashUtils._

  val nameConverter = ColumnNamesConverterFactory.create(namingConvention)

  private val NormalizedHashSeq: Seq[String] = HashSeq
    .map(s => HashColPrefix + HashNameNorm(s))
    .map(s => nameConverter.namingConvention(s))

  private val StructFieldsArray = StructType(
    NormalizedHashSeq
      .map(s => StructField(s, DataTypes.StringType, nullable = true))
      .toArray
  )

  private val documentToStrings: SolrDocument => Seq[String] =
    (document: SolrDocument) => NormalizedHashSeq.map(s => getDocumentFieldValueOrEmptyString(document, s))


  override def load(): DataFrame = {
    val clientBuilder = SolrClientBuilder().setZKServers(config.zkServers)
      .setZKSolrChroot(config.zkSolrZNode)
      .setDefaultCollection(config.collection)
    val clientBuilderWithSec = config.krbConfig
      .map(clientBuilder.setJaas)
      .getOrElse(clientBuilder)
    val client = clientBuilderWithSec.build()

    val documents = getSolrDocumentList(client, QueryStartRow)
    val idHashValueSeq = documents.map((d: SolrDocument) =>
      Row.fromSeq(documentToStrings(d)))
    val dataRdd = sparkSession.sparkContext.parallelize(idHashValueSeq)
    sparkSession.createDataFrame(dataRdd, StructFieldsArray)
  }

  private def getDocumentFieldValueOrEmptyString(document: SolrDocument, fieldName: String) = {
    val fieldValue = Option(document.getFirstValue(fieldName).asInstanceOf[String])
    fieldValue.orNull
  }

  def getSolrDocumentList(client: CloudSolrClient, queryStartRow: Int): SolrDocumentList = {
    val queryParams = new MapSolrParams(builtQueryParamMap(queryStartRow))
    val response = client.query(queryParams)
    val documentList = response.getResults
    if (documentList.nonEmpty) {
      documentList.addAll(getSolrDocumentList(client, queryStartRow + RowsValue))
    }
    documentList
  }

  def builtQueryParamMap(queryStartRow: Int) = {
    val queryParamMap = new util.HashMap[String, String]()
    queryParamMap.put(QueryKey, QueryAllValue)
    queryParamMap.put(FieldKey, NormalizedHashSeq.mkString(Comma))
    queryParamMap.put(StartKey, queryStartRow.toString)
    queryParamMap.put(RowsKey, RowsValue.toString)
    queryParamMap
  }
}

object SolrHashReader {

  import ai.datahunters.md.util.HashUtils._

  val QueryAllValue = "*:*"
  val Comma = ","
  val QueryKey = "q"
  val FieldKey = "fl"
  val RowsKey = "rows"
  val RowsValue = 10
  val StartKey = "start"
  val QueryStartRow = 0

  val HashSeq: Seq[String] = Seq(CRC32, MD5, SHA_1, SHA_224, SHA_256, SHA_384, SHA_512)

}