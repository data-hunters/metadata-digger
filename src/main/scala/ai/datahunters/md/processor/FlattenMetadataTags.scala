package ai.datahunters.md.processor

import ai.datahunters.md.schema.MetadataSchemaConfig.{MetadataCol, MetadataContentCol}
import ai.datahunters.md.schema._
import ai.datahunters.md.udf.Extractors
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.types.StructType

case class FlattenMetadataTags(colPrefix: String, includeDirName: Boolean = false, allowedTags: Option[Seq[String]] = None) extends Processor {
  import org.apache.spark.sql.functions._
  import FlattenMetadataTags._
  import Extractors._

  override def execute(inputDF: DataFrame): DataFrame = {
    val selectedTags = retrieveTags(inputDF)
    val columns = SchemaConfig.dfExistingColumns(inputDF, Seq(MetadataCol)) ++ Seq(s"${MetadataCol}.*")
    val selectMetadataTagsUDF = selectMetadataTagsFromDirs(colPrefix, includeDirName, selectedTags)

    inputDF.withColumn(MetadataCol, selectMetadataTagsUDF(col(MetadataCol)))
      .select(columns.head, columns.tail:_*)
      .withColumn(MetadataContentCol, concat_ws(" ", buildConcatTagList(selectedTags, colPrefix):_*))
  }

  private def buildConcatTagList(selectedTags: Seq[String], colPrefix: String): Seq[Column] = {
    selectedTags.map(t => col(colPrefix + t)) ++ Seq(col(BinaryInputSchemaConfig.FilePathCol))
  }

  private def retrieveTags(inputDF: DataFrame): Seq[String] = {
    allowedTags.getOrElse({
      inputDF.cache()
      val availableTags = inputDF
        .select(selectMetadataTagNames(includeDirName)(col(MetadataCol)).as(TempTagsCol))
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
