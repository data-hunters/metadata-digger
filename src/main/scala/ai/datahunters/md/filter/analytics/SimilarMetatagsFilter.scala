package ai.datahunters.md.filter.analytics

import ai.datahunters.md.TagValueType.TagValueType
import ai.datahunters.md.filter.Filter
import ai.datahunters.md.schema.EmbeddedMetadataSchemaConfig
import ai.datahunters.md.udf.analytics.Similarities
import ai.datahunters.md.{MetatagSimilarity, TagValueType}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, lit}

/**
  * Calculate distances between values of base file's tags and all other in input DataFrame.
  * All files/rows that have less than minPassing similar tags are filtered out from DataFrame.
  *
  * @param tags
  * @param minPassing
  */
case class SimilarMetatagsFilter(private val tags: Seq[MetatagSimilarity[Any]], private val minPassing: Int) extends Filter {

  import EmbeddedMetadataSchemaConfig.FullTagsCol
  import Similarities._

  override def execute(inputDF: DataFrame): DataFrame = {
    val similaritiyCountsUDFs = tags.groupBy(_.definition.valueType)
      .map(tagsSubGroup => selectUDF(tagsSubGroup._1, tagsSubGroup._2))
      .foldLeft(lit(0))((col1, udf2) => col1.plus(udf2(col(FullTagsCol))))
    inputDF.filter(similaritiyCountsUDFs.geq(minPassing))
  }

  private def selectUDF(tagValueType: TagValueType, tags: Seq[MetatagSimilarity[Any]]): UserDefinedFunction = tagValueType match {
    case TagValueType.FLOAT => checkSimilarFloats(tags.map(_.asInstanceOf[MetatagSimilarity[Float]]))
    case TagValueType.INT => checkSimilarInts(tags.map(_.asInstanceOf[MetatagSimilarity[Int]]))
    case TagValueType.LONG => checkSimilarLongs(tags.map(_.asInstanceOf[MetatagSimilarity[Long]]))
    case TagValueType.STRING => checkLevDistance(tags.map(_.asInstanceOf[MetatagSimilarity[String]]))
  }

}
