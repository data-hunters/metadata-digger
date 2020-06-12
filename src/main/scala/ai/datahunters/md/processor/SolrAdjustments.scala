package ai.datahunters.md.processor
import ai.datahunters.md.schema.EmbeddedMetadataSchemaConfig
import ai.datahunters.md.schema.MetadataSchemaConfig.MetadataContentCol
import ai.datahunters.md.udf.Extractors
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
  * Adjust DataFrame to SolrForeachWriter:
  * * Change fields with metadata directories to Map[String, String]
  * * Add column prefix to all metadata inside Map
  * * Build field containing concatenation of all values of metadata tags.
  * @param colPrefix
  * @param includeDirName
  * @param includeMetadataContent
  */
case class SolrAdjustments(colPrefix: String,
                           includeDirName: Boolean = false,
                           includeMetadataContent: Boolean = false) extends Processor {
  import EmbeddedMetadataSchemaConfig._
  import SolrAdjustments._
  override def execute(inputDF: DataFrame): DataFrame = {
    val embeddedMapstoMapUDF = Extractors.embeddedMapsToMap(colPrefix, includeDirName)
    val embeddedMapsDF = inputDF.withColumn(MetadataCol, embeddedMapstoMapUDF(col(MetadataCol)))
    if (includeMetadataContent) {
      embeddedMapsDF.withColumn(MetadataContentCol, array_join(map_values(col(MetadataCol)), ColumnsContentDelimiter))
    } else {
      embeddedMapsDF
    }
  }
}

object SolrAdjustments {
  val ColumnsContentDelimiter = " "
}
