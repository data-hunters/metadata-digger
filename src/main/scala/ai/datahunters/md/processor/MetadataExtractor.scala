package ai.datahunters.md.processor
import ai.datahunters.md.schema.{BinaryInputSchemaConfig, EmbeddedMetadataSchemaConfig}
import ai.datahunters.md.udf.Extractors
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

/**
  * Extracts all existing metadata into embedded structure kept in single DataFrame column.
  * @param dropFileColumn
  */
case class MetadataExtractor(dropFileColumn: Boolean = true) extends Processor {

  override def execute(inputDF: DataFrame): DataFrame = {
    val extractMetadataUDF = Extractors.extractMetadata()
    val outputDF = inputDF.withColumn(EmbeddedMetadataSchemaConfig.MetadataCol, extractMetadataUDF(col(BinaryInputSchemaConfig.FileCol)))
    if (dropFileColumn)
      outputDF.drop(BinaryInputSchemaConfig.FileCol)
    else
      outputDF
  }
}

object MetadataExtractor {

  val AllowedDirs = Seq(
    "JPEG",
    "Exif IFD0",
    "Exif SubIFD",
    "Nikon Makernote",
    "Olympus Makernote",
    "Canon Makernote",
    "Panasonic Makernote",
    "PrintIM",
    "Interoperability",
    "GPS",
    "Exif Thumbnail",
    "Huffman",
    "File Type",
    "MP4",
    "MP4 Video",
    "MP4 Sound"
  )


}