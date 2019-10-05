package ai.datahunters.md.udf

import java.io.ByteArrayInputStream

import ai.datahunters.md.schema._
import com.drew.imaging.ImageMetadataReader
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

object Extractors {

  import ai.datahunters.md.util.TextUtils._

  def selectMetadata(allowedDirs: Seq[String]): UserDefinedFunction = {
    udf(selectMetadataT(allowedDirs) _, MetadataSchemaConfig(allowedDirs).schema())
  }

  def extractMetadata(): UserDefinedFunction = {
    udf(extractMetadataT _, EmbeddedMetadataSchemaConfig().schema())
  }

  def selectMetadataTagNames(): UserDefinedFunction = udf(selectMetadataTagNamesT _)

  def selectMetadataTagsFromDirs(allowedTags: Seq[String]): UserDefinedFunction= {
    udf(selectMetadataTagsFromDirsT _, MetadataTagsSchemaConfig(allowedTags).schema())
  }

  def selectMetadataTags(allowedTags: Seq[String]): UserDefinedFunction= {
    udf(selectMetadataTagsT _, MetadataTagsSchemaConfig(allowedTags).schema())
  }


  def extractMetadataT(file: Array[Byte]): Row = {
    val metadata = ImageMetadataReader.readMetadata(new ByteArrayInputStream(file))

    import scala.collection.JavaConversions._
    val dirs = metadata.getDirectories.toSeq.map(_.getName)
    var tagsCount = 0
    val tags = metadata.getDirectories.map(directory => {
      val dirTags = directory.getTags.map(tag => {
        tagsCount += 1
        (safeName(camelCase(tag.getTagName)) -> tag.getDescription)
      }).toMap
      (safeName(camelCase(directory.getName)) -> dirTags)
    }).toMap
    Row.fromTuple(tags, dirs, tagsCount)
  }


  def selectMetadataT(allowedDirs: Seq[String])(tags: Map[String, Map[String, String]]) = {
    val orderedMetadata: Seq[Map[String, String]] = allowedDirs.map(dir => {
      tags.get(dir).getOrElse(Map[String, String]())
    })
    Row.fromSeq(orderedMetadata)
  }

  def selectMetadataTagNamesT(directories: Row): Seq[String] = {
    val columns = SchemaConfig.rowExistingColumns(directories)
    val tags: Map[String, Map[String, String]] = directories.getValuesMap(columns)
    tags.flatMap(_._2).keySet.toSeq
  }

  def selectMetadataTagsFromDirsT(allowedTags: Seq[String])(directories: Row): Row = {
    val columns = SchemaConfig.rowExistingColumns(directories)
    val tags: Map[String, Map[String, String]] = directories.getValuesMap(columns)
    val allTags: Map[String, String] = tags.flatMap(_._2)
    val orderedMetadata: Seq[String] = allowedTags.map(tag => {
      allTags.get(tag).getOrElse("")
    })
    Row.fromSeq(orderedMetadata)
  }

  def selectMetadataTagsT(allowedTags: Seq[String])(tags: Map[String, Map[String, String]]): Row = {
    val allTags: Map[String, String] = tags.flatMap(_._2)
      val orderedMetadata: Seq[String] = allowedTags.map(tag => {
        allTags.get(tag).getOrElse("")
      })
      Row.fromSeq(orderedMetadata)
  }

}
