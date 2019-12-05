package ai.datahunters.md.udf

import ai.datahunters.md.MandatoryTag
import ai.datahunters.md.schema.SchemaConfig
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.udf

object Filters {
  val EMPTY_STRING = ""

  val notEmptyMapUDF = udf((inputMap: Map[String, String]) => !inputMap.isEmpty)

  def notEmptyTagValueUDF(mandatoryTags: MandatoryTag) =
  udf((dictionaries: Row) => {
      val dirs = SchemaConfig.rowExistingColumns(dictionaries)
      val dirTagValueMap: Map[String, Map[String, String]] = dictionaries.getValuesMap(dirs)
      !dirTagValueMap.getOrElse(mandatoryTags.directory, Map())
        .getOrElse(mandatoryTags.tag, EMPTY_STRING)
        .equals(EMPTY_STRING)
    })

}
