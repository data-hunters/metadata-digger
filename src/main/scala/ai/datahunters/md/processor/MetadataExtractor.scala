package ai.datahunters.md.processor
import ai.datahunters.md.schema.{BinaryInputSchemaConfig, EmbeddedMetadataSchemaConfig}
import ai.datahunters.md.udf.Extractors
import com.drew.imaging.FileType
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

/**
  * Extracts all existing metadata into embedded structure kept in single DataFrame column.
  * @param dropFileColumn
  */
case class MetadataExtractor(dropFileColumn: Boolean = true) extends Processor {

  import Extractors.extractMetadata

  override def execute(inputDF: DataFrame): DataFrame = {
    val extractMetadataUDF = extractMetadata()
    val outputDF = inputDF.withColumn(
      EmbeddedMetadataSchemaConfig.MetadataCol,
      extractMetadataUDF(col(BinaryInputSchemaConfig.FilePathCol), col(BinaryInputSchemaConfig.FileCol))
    )
      .withColumn(EmbeddedMetadataSchemaConfig.FileTypeCol, col(EmbeddedMetadataSchemaConfig.FullFileTypeCol))
      .filter(col(EmbeddedMetadataSchemaConfig.FullFileTypeCol).notEqual(FileType.Unknown.toString))
    if (dropFileColumn)
      outputDF.drop(BinaryInputSchemaConfig.FileCol).cache()
    else
      outputDF.cache()
  }
}
