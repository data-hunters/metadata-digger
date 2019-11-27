package ai.datahunters.md.filter

import ai.datahunters.md.schema.EmbeddedMetadataSchemaConfig._
import ai.datahunters.md.schema.SchemaConfig
import ai.datahunters.md.util.StructuresTransformations
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{DataFrame, Row}

/**
  * Filter all data which don't have value for specified mandatory tags.
  * @param mandatoryTags Information of mandatory tags can be provide in config file using special format [dir.tag]
  *                      placed in filter.mandatoryTags property
  */
class NotEmptyTagFilter(mandatoryTags: Seq[String]) extends Filter {

  import ai.datahunters.md.udf.Filters._

  override def execute(inputDF: DataFrame): DataFrame = {
    inputDF.cache()
    val tmpTagsCol = "tmpTags"
    val getAvailableDirTagListUDF = NotEmptyTagFilter.selectMetadataTagNames(col(MetadataCol))
    inputDF
      .withColumn(tmpTagsCol, getAvailableDirTagListUDF)
      .filter(notEmptyValueUDF(mandatoryTags)(col(tmpTagsCol)))
      .drop(tmpTagsCol)
  }
}

object NotEmptyTagFilter {
  def selectMetadataTagNames: UserDefinedFunction = udf(getDirTagStringWithNotNullValues _)

  def getDirTagStringWithNotNullValues(directories: Row): Seq[String] = {
    val columns = SchemaConfig.rowExistingColumns(directories)
    val map = directories.getValuesMap(columns)
    StructuresTransformations.concatKeysToSeqIfValueNotNull(map)
  }

}