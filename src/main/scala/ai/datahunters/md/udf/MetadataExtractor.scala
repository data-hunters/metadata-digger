package ai.datahunters.md.udf

import java.io.ByteArrayInputStream

import ai.datahunters.md.util.DateTimeUtils.MainDateTimeFormatter
import com.drew.imaging.{FileType, ImageMetadataReader}
import com.drew.metadata.exif.GpsDirectory
import org.apache.spark.sql.Row

class MetadataExtractor {

  import MetadataExtractor._

  def extract(file: Array[Byte]): Row = {
    val metadata = ImageMetadataReader.readMetadata(new ByteArrayInputStream(file))
    import scala.collection.JavaConversions._
    val dirs = metadata.getDirectories.toSeq.map(_.getName)
    var tagsCount = 0
    val tags = metadata.getDirectories.map(directory => {
      val customTags = directory match {
        case d: GpsDirectory => parseCustomGeolocation(d)
        case other => Seq()
      }
      val dirTags = directory.getTags.map(tag => {
        tagsCount += 1
        (tag.getTagName -> tag.getDescription)
      }) ++ customTags
      (directory.getName -> dirTags.toMap)
    }).toMap
    val fileType = tags.get(FileTypeDir)
      .map(_.get(FileTypeTag).getOrElse(UnknownType))
      .getOrElse(UnknownType)
    Row.fromTuple(tags, dirs, tagsCount, fileType)
  }
}

object MetadataExtractor {

  val FileTypeDir = "File Type"
  val FileTypeTag = "Detected File Type Name"
  val UnknownType = FileType.Unknown.toString
  val GpsLocationFieldTag = "Location"
  val GpsLocationDateTimeTag = "DateTime"

  private def parseCustomGeolocation(dir: GpsDirectory): Seq[(String, String)] = {
    val location = if (dir.getGeoLocation != null){
      Seq((GpsLocationFieldTag -> s"${dir.getGeoLocation.getLatitude},${dir.getGeoLocation.getLongitude}"))
    } else Seq()
    val dt = if (dir.getGpsDate != null) {
      Seq((GpsLocationDateTimeTag -> s"${MainDateTimeFormatter.format(dir.getGpsDate)}"))
    } else Seq()
    location ++ dt
  }
}