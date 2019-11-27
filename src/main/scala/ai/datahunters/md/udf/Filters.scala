package ai.datahunters.md.udf

import org.apache.spark.sql.functions.udf

object Filters {

  val notEmptyMapUDF = udf((inputMap: Map[String, String]) => !inputMap.isEmpty)


  def notEmptyValueUDF(mandatoryTags: Seq[String]) =
    udf((inputSeq: Seq[String]) => inputSeq.intersect(mandatoryTags).nonEmpty)

}
