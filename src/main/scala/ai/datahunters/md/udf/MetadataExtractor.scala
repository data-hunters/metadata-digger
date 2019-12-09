package ai.datahunters.md.udf

import java.io.ByteArrayInputStream

import ai.datahunters.md.MetadataInfo
import ai.datahunters.md.util.DateTimeUtils.MainDateTimeFormatter
import ai.datahunters.md.util.TextUtils
import com.drew.imaging.{FileType, ImageMetadataReader}
import com.drew.metadata.exif.GpsDirectory
import com.drew.metadata.{Directory, StringValue, Tag}
import org.apache.spark.sql.Row
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer

object MetadataExtractor {


  val FileTypeDir = "File Type"
  val FileTypeTag = "Detected File Type Name"
  val UnknownType = FileType.Unknown.toString
  val GpsLocationFieldTag = "MD Location"
  val GpsLongitudeFieldTag = "MD Location Long F"
  val GpsLatitudeFieldTag = "MD Location Lat F"
  val GpsLocationDateTimeTag = "MD DateTime"
  private val Logger = LoggerFactory.getLogger(MetadataExtractor.getClass)

  def extract(file: Array[Byte]): MetadataInfo = {
    val metadata = ImageMetadataReader.readMetadata(new ByteArrayInputStream(file))
    import scala.collection.JavaConversions._
    val dirs = metadata.getDirectories.toSeq.map(_.getName)
    val tagNames = ArrayBuffer[String]()
    var tagsCount = 0
    val tags = metadata.getDirectories.map(directory => {
      val customTags = directory match {
        case d: GpsDirectory => parseCustomGeolocation(d)
        case other => Seq()
      }
      val dirTags = directory.getTags.map(tag => {
        tagsCount += 1
        val tagVal = if (tag.getDescription != null) {
          tag.getDescription.replace("\n", "\\n")
        } else handleNullVal(directory, tag)
        tagNames.append(tag.getTagName)
        (TextUtils.safeName(tag.getTagName) -> tagVal)
      }) ++ customTags
      (TextUtils.safeName(directory.getName) -> dirTags.toMap)
    }).toMap
    val fileType = tags.get(FileTypeDir)
      .map(_.get(FileTypeTag).getOrElse(UnknownType))
      .getOrElse(UnknownType)
    MetadataInfo(tags, dirs, tagNames, tagsCount, fileType)
  }

  /**
    * Very special cases where metadata-extractor lib cannot determine string representation of value
    *
    * @param directory
    * @param tag
    * @return
    */
  private def handleNullVal(directory: Directory, tag: Tag): String = directory.getObject(tag.getTagType) match {
    case v: Array[Int] => {
      // Weird case where metadata-extractor cannot determine double value because it is represented as two-element
      // array. It's for instance in case of Exif SubIFD.Exif Image Width
      if (v.length > 2) v.mkString(",") else if (v.length > 0) v(0).toString else null
    }
    case v: StringValue => v.toString
    case v: Array[Double] => v.mkString(" ")
    case v: Array[Long] => v.mkString(" ")
    case v: Array[Short] => v.mkString(" ")
    case other => {
      val tagName = TextUtils.safeName(tag.getTagName)
      val dirName = TextUtils.safeName(tag.getDirectoryName)
      Logger.warn(s"Cannot parse value of tag: ${tagName} (directory: ${dirName}, number: ${tag.getTagType}, hex: ${tag.getTagTypeHex}). It will have null value.")
      null
    }
  }

  private def parseCustomGeolocation(dir: GpsDirectory): Seq[(String, String)] = {
    val location = if (dir.getGeoLocation != null){
      Seq(
        (GpsLocationFieldTag -> s"${dir.getGeoLocation.getLatitude},${dir.getGeoLocation.getLongitude}"),
        (GpsLatitudeFieldTag -> dir.getGeoLocation.getLatitude.toString),
        (GpsLongitudeFieldTag -> dir.getGeoLocation.getLongitude.toString)
      )
    } else Seq()
    val dt = if (dir.getGpsDate != null) {
      Seq((GpsLocationDateTimeTag -> s"${MainDateTimeFormatter.format(dir.getGpsDate)}"))
    } else Seq()
    location ++ dt
  }
}