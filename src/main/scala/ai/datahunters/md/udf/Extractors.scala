package ai.datahunters.md.udf

import java.io.ByteArrayInputStream
import java.util.Date

import ai.datahunters.md.schema._
import ai.datahunters.md.util.StructuresTransformations
import com.drew.imaging.{FileType, FileTypeDetector, ImageMetadataReader}
import com.drew.lang.GeoLocation
import com.drew.metadata.exif.GpsDirectory
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.slf4j.LoggerFactory

object Extractors {

  import Transformations._

  /**
    * UDF retrieving tags from nested map (Map[String, Map[String, String]])
    * to list of flat maps (Map[String, String]).
    * Input argument for UDF - column of type: Map[String, Map[String, String]]
    * UDF output: Row (StructType), read {@link MetadataSchemaConfig} for output schema
    *
    * @param allowedDirs Alldirectories that will be included in result
    * @return
    */
  def selectMetadata(allowedDirs: Seq[String]): UserDefinedFunction = {
    udf(Transformations.selectMetadataT(allowedDirs) _, MetadataSchemaConfig(allowedDirs).schema())
  }

  /**
    * UDF retrieving metadata from binary file to nested map (Mep[String, Map[String, String]])
    * where each main key is directory/group of tags (e.g. "Exif IFD0"), each nested key is tag name
    * and value is valueof particular tag.
    * Input argument for UDF - column of type Array[Byte]
    * UDF output: Row (StructType), read {@link EmbeddedMetadataSchemaConfig} for output schema
    *
    * @return
    */
  def extractMetadata(): UserDefinedFunction = {
    udf(Transformations.extractMetadataT() _, EmbeddedMetadataSchemaConfig().schema())
  }

  /**
    * UDF retrieving list of all tag names from all columns passed in input Row.
    * Input argument for UDF - column of type: Row (StructType)
    * UDF output: Seq[String]
    *
    * @param includeDirName
    * @return
    */
  def selectMetadataTagNames(includeDirName: Boolean, dirTagNameSeparator: Option[String] = None): UserDefinedFunction = {
    udf(selectMetadataTagNamesT(includeDirName, dirTagNameSeparator) _)
  }

  /**
    * UDF retrieving tags from all columns passed in input Row.
    * Input argument for UDF - column of type: Row (StructType)
    * UDF output: Row (StructType), read {@link MetadataTagsSchemaConfig} for output schema.
    *
    * @param colPrefix
    * @param includeDirName
    * @param allowedTags
    * @return
    */
  def selectMetadataTagsFromDirs(colPrefix: String, includeDirName: Boolean, allowedTags: Seq[String]): UserDefinedFunction= {
    udf(selectMetadataTagsFromDirsT(includeDirName, allowedTags) _, MetadataTagsSchemaConfig(colPrefix, allowedTags).schema())
  }

  /**
    * UDF retrieving tags from nested map (Map[String, Map[String, String]]).
    * Input argument for UDF - column of type: Map[String, Map[String, String]]
    * UDF output: Row (StructType), read {@link MetadataTagsSchemaConfig} for output schema.
    *
    * @param colPrefix
    * @param allowedTags
    * @return
    */
  def selectMetadataTags(colPrefix: String, allowedTags: Seq[String]): UserDefinedFunction= {
    udf(selectMetadataTagsT(allowedTags) _, MetadataTagsSchemaConfig(colPrefix, allowedTags).schema())
  }


  /**
    * UDF flattening Map[String, Map[String, String]] into Map[String, String].
    * Input argument for UDF - column of type Map[String, Map[String, String]]
    * UDF output: Map[String, String]
    *
    * @param colPrefix
    * @param includeDirs
    * @return
    */
  def embeddedMapsToMap(colPrefix: String, includeDirs: Boolean): UserDefinedFunction = {
    udf(embeddedMapsToMapT(colPrefix, includeDirs) _)
  }

  /**
    * Contains methods implementing actual UDF transformations.
    */
  object Transformations {
    val Logger = LoggerFactory.getLogger(Transformations.getClass)

    def extractMetadataT()(path:String, file: Array[Byte]): Row = {
      try {
        val mdInfo = MetadataExtractor.extract(file)
        Row.fromTuple(mdInfo.tags, mdInfo.dirNames, mdInfo.tagNames, mdInfo.tagsCount, mdInfo.fileType)
      } catch {
        case e: Exception => {
          Logger.warn(s"Error occurred during metadata extraction for image: $path (Message: {}). Ignoring file...", e.getMessage)
          Row.fromTuple(Map(), Seq(), Seq(), 0, FileType.Unknown.toString)
        }
      }
    }

    def selectMetadataT(allowedDirs: Seq[String])(tags: Map[String, Map[String, String]]) = {
      val orderedMetadata: Seq[Map[String, String]] = allowedDirs.map(dir => {
        tags.get(dir).getOrElse(Map[String, String]())
      })
      Row.fromSeq(orderedMetadata)
    }

    def selectMetadataTagNamesT(includeDirName: Boolean, dirTagNameSeparator: Option[String] = None)(directories: Row): Seq[String] = {
      val columns = SchemaConfig.rowExistingColumns(directories)
      val tags: Map[String, Map[String, String]] = directories.getValuesMap(columns)
      if (includeDirName) {
        import StructuresTransformations.concatKeysToSeq
        dirTagNameSeparator
          .map(separator => concatKeysToSeq(tags, separator))
          .getOrElse(concatKeysToSeq(tags))
      } else {
        tags.flatMap(_._2.keySet).toSeq
      }
    }

    def embeddedMapsToMapT(colPrefix: String, includeDirs: Boolean)(directories: Row): Map[String, String] = {
      val columns = SchemaConfig.rowExistingColumns(directories)
      val tags: Map[String, Map[String, String]] = directories.getValuesMap(columns)
      if (includeDirs) {
        tags.flatMap(tagsMap => {
          tagsMap._2.map(kv => s"$colPrefix${tagsMap._1} ${kv._1}" -> kv._2)
        })
      } else {
        tags.flatMap(tagsMap => tagsMap._2.map(tv => s"$colPrefix${tv._1}" -> tv._2))
      }
    }

    def selectMetadataTagsFromDirsT(includeDirs: Boolean, allowedTags: Seq[String])(directories: Row): Row = {
      val columns = SchemaConfig.rowExistingColumns(directories)
      val tags: Map[String, Map[String, String]] = directories.getValuesMap(columns)
      val allTags: Map[String, String] = if (includeDirs) {
        StructuresTransformations.concatKeys(tags)
      } else {
        tags.flatMap(_._2)
      }
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


}
