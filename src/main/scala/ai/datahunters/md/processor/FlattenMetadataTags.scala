package ai.datahunters.md.processor

import ai.datahunters.md.config.processing.ProcessingConfig
import ai.datahunters.md.schema.MetadataSchemaConfig.{MetadataCol, MetadataContentCol}
import ai.datahunters.md.schema._
import ai.datahunters.md.udf.Extractors
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.types.{ArrayType, StructType}

/**
  * Retrieve all metadata columns from nested structures to the root
  *
  * @param colPrefix Prefix that should be added to all metadata
  * @param includeDirName Add directory/group name to all metadata tags
  * @param removeArrays Remove all columns that have type of array
  * @param allowedTags List of allowed tags to include in the final DataFrame, include all if None.
  */
case class FlattenMetadataTags(colPrefix: String,
                               includeDirName: Boolean = false,
                               removeArrays: Boolean = false,
                               includeMetadataContent: Boolean = false,
                               allowedTags: Option[Seq[String]] = None,
                               flattenArrays: Boolean = false,
                               arraysDelimiter: String = ",") extends Processor {
  import org.apache.spark.sql.functions._
  import FlattenMetadataTags._
  import Extractors._

  private val flattenArraysProcessor = FlattenArrays(arraysDelimiter)(_)

  override def execute(inputDF: DataFrame): DataFrame = {
    val selectedTags = retrieveTags(inputDF)
    checkColumnsUniqueness(selectedTags)
    val columns = SchemaConfig.dfExistingColumns(inputDF, Seq(MetadataCol)) ++ Seq(s"${MetadataCol}.*")
    val selectMetadataTagsUDF = selectMetadataTagsFromDirs(colPrefix, includeDirName, selectedTags)

    val flattenDF = inputDF.withColumn(MetadataCol, selectMetadataTagsUDF(col(MetadataCol)))
      .select(columns.head, columns.tail:_*)
    val arrayFieldsToFlatten = selectArrayColumns(flattenDF)
    val afterArraysFlatteningDF = if (flattenArrays) {
      flattenArraysProcessor(arrayFieldsToFlatten).execute(flattenDF)
    } else {
      flattenDF
    }
    val transformedDF = if (includeMetadataContent) {
      afterArraysFlatteningDF.withColumn(MetadataContentCol, concat_ws(ColumnsContentDelimiter, buildConcatTagList(selectedTags, colPrefix):_*))
    } else {
      afterArraysFlatteningDF
    }

    selectFinalColumns(transformedDF)
  }

  private def checkColumnsUniqueness(columns: Seq[String]): Unit = {
    val lowercased = columns.map(_.toLowerCase)
    if (lowercased.distinct.size < lowercased.size) {
      throw new TheSameTagNamesException(s"Two different Metadata Directories contain the same tag name, please set property - ${ProcessingConfig.IncludeDirectoriesInTagNamesKey} to true to avoid this problem.")
    }
  }

  private def selectArrayColumns(df: DataFrame): Seq[String] = {
    df.schema
      .fields
      .filter(f => f.dataType.isInstanceOf[ArrayType])
      .map(_.name)
  }

  private def selectFinalColumns(df: DataFrame): DataFrame = if (removeArrays) {
    val simpleFields = df.schema
      .fields
      .filter(f => !f.dataType.isInstanceOf[ArrayType] && !f.dataType.isInstanceOf[StructType] )
      .map(fn => col(fn.name))

    df.select(simpleFields:_*)
  } else {
    df
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

  case class TheSameTagNamesException(msg: String) extends RuntimeException(msg)

  private val TempTagCol = "Tag"
  private val TempTagsCol = "Tags"

  private val ColumnsContentDelimiter = " "

}
