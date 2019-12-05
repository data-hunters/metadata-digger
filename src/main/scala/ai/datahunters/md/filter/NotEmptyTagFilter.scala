package ai.datahunters.md.filter

import ai.datahunters.md.MandatoryTag
import ai.datahunters.md.schema.MetadataSchemaConfig.MetadataCol
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

/**
  * Filter all data which don't have value for specified mandatory tags.
  *
  * @param mandatoryTags Information of mandatory tags can be provide in config file using special format [dir.tag]
  *                      placed in filter.mandatoryTags property
  */
class NotEmptyTagFilter(mandatoryTags: Seq[MandatoryTag]) extends Filter {

  import ai.datahunters.md.udf.Filters._

  override def execute(inputDF: DataFrame): DataFrame = {
    var resultDF = inputDF
    mandatoryTags.foreach(mt => {
      resultDF = inputDF.filter(notEmptyTagValueUDF(mt)(col(MetadataCol)))
    })
    resultDF
  }
}