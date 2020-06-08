package ai.datahunters.md.filter

import org.apache.spark.sql.DataFrame
import ai.datahunters.md.udf.Filters._
import ai.datahunters.md.schema.EmbeddedMetadataSchemaConfig.FileTypeCol
import org.apache.spark.sql.functions._

/**
  * Filter images that do not have specified allowed extension
  */

class NotEmptyExtensionFilter(allowedExtensions: Seq[String]) extends Filter {

  override def execute(inputDF: DataFrame): DataFrame = {
    inputDF.filter(notEmptyExtensionUDF(allowedExtensions)(col(FileTypeCol)))
  }

}
