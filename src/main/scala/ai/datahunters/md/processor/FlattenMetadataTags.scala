package ai.datahunters.md.processor

import ai.datahunters.md.schema.{EmbeddedMetadataSchemaConfig, MetadataSchemaConfig, SchemaConfig}
import ai.datahunters.md.udf.Extractors
import org.apache.spark.sql.DataFrame

case class FlattenMetadataTags(allowedTags: Option[Seq[String]] = None) extends Processor {
  import org.apache.spark.sql.functions._
  import FlattenMetadataTags._
  import ai.datahunters.md.schema.MetadataTagsSchemaConfig._

  override def execute(inputDF: DataFrame): DataFrame = {
    val selectedDirs = retrieveTags(inputDF)
    val columns = SchemaConfig.dfExistingColumns(inputDF, Seq(MetadataCol)) ++ Seq(s"${MetadataCol}.*")
    val selectMetadataTagsUDF = Extractors.selectMetadataTagsFromDirs(selectedDirs)
    inputDF.withColumn(MetadataCol, selectMetadataTagsUDF(col(MetadataSchemaConfig.MetadataCol)))
      .select( columns.head, columns.tail:_*)
  }

  private def retrieveTags(inputDF: DataFrame): Seq[String] = {
    allowedTags.getOrElse({
      inputDF.cache()
      val availableTags = inputDF
        .select(Extractors.selectMetadataTagNames()(col(MetadataSchemaConfig.MetadataCol)).as(TempTagsCol))
        .select(explode(col(TempTagsCol)).as(TempTagCol))
        .distinct()
        .collect()
        .map(_.getAs[String](TempTagCol))
      availableTags.toSeq
    })
  }
}

object FlattenMetadataTags {

  private val TempTagCol = "Tag"
  private val TempTagsCol = "Tags"

}
