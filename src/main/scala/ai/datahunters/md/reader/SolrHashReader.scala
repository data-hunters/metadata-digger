package ai.datahunters.md.reader

import java.util

import ai.datahunters.md.config.writer.SolrWriterConfig
import ai.datahunters.md.util.SolrClientBuilder
import org.apache.solr.client.solrj.impl.CloudSolrClient
import org.apache.solr.common.SolrDocument
import org.apache.solr.common.params.MapSolrParams
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
                          config: SolrWriterConfig) extends PipelineSource {

  import ai.datahunters.md.reader.SolrHashReader._

  private val documentToStrings: SolrDocument => Seq[String] =
    (document: SolrDocument) => HashSeq.map(s => getDocumentFieldValueOrEmptyString(document, s))


  override def load(): DataFrame = {
    val client = SolrClientBuilder().setZKServers(config.zkServers)
      .setZKSolrChroot(config.zkSolrZNode)
      .setDefaultCollection(config.collection)
      .build()

    val documents = getSolrDocumentList(client)

    val idHashValueSeq = documents.map((d:SolrDocument) =>
      Row.fromSeq(documentToStrings(d)))

    val dataRdd = sparkSession.sparkContext.parallelize(idHashValueSeq)
    sparkSession.createDataFrame(dataRdd, Schema)
  }

  private def getDocumentFieldValueOrEmptyString(document: SolrDocument, fieldName: String) = {
    val fieldValue = Option(document.getFirstValue(fieldName).asInstanceOf[String])
    fieldValue.orNull
  }

  def getSolrDocumentList(client: CloudSolrClient) = {
    val queryParamMap = new util.HashMap[String, String]()
    queryParamMap.put(QueryKey, QueryAllValue)
    queryParamMap.put(FieldKey, HashSeq.mkString(Comma))
    val queryParams = new MapSolrParams(queryParamMap)
    val response = client.query(queryParams)
    response.getResults
  }
}

object SolrHashReader {
  val QueryAllValue = "*:*"
  val Comma = ","
  val QueryKey = "q"
  val FieldKey = "fl"

  val Crc32 = "hash_crc32"
  val Md5 = "hash_md5"
  val Sha1 = "hash_sha1"
  val Sha224 = "hash_sha224"
  val Sha256 = "hash_sha256"
  val Sha384 = "hash_sha384"
  val Sha512 = "hash_sha512"

  val HashSeq: Seq[String] = Seq(Crc32, Md5, Sha1, Sha224, Sha256, Sha384, Sha512)

  private val StructFieldsArray = HashSeq.map(s => StructField(s, DataTypes.StringType, nullable = true)).toArray

  val Schema = StructType(
    StructFieldsArray
  )
}