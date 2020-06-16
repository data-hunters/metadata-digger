package ai.datahunters.md.filter

import org.apache.spark.sql.DataFrame
import ai.datahunters.md.udf.Filters._
import ai.datahunters.md.schema.EmbeddedMetadataSchemaConfig.FileTypeCol
import org.apache.spark.sql.functions._

/**
  * Filter images that do not have specified allowed extension
  */

case class NotEmptyFileTypeFilter(allowedFileTypes: Option[Seq[String]] = None) extends Filter {

  override def execute(inputDF: DataFrame): DataFrame = {
    val fileTypes = allowedFileTypes.getOrElse({
      return inputDF;
    })
    inputDF.filter(filterAllowedFileTypesUDF(fileTypes)(col(FileTypeCol)))
  }

}
