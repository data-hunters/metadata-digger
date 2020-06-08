package ai.datahunters.md.config.processing

import ai.datahunters.md.config.{ConfigLoader, Writer}
import ai.datahunters.md.udf.image.ImageProcessing.ThumbnailSize
import com.typesafe.config.Config

case class ProcessingConfig(allowedDirectories: Option[Seq[String]],
                            allowedTags: Option[Seq[String]],
                            mandatoryTags: Option[Seq[String]],
                            hashList: Option[Seq[String]],
                            allowedExtensions: Option[Seq[String]],
                            processHashComparator: Boolean = false,
                            namingConvention: String,
                            metadataColumnsPrefix: String,
                            includeDirsInTags: Boolean,
                            outputFormat: String,
                            includeMetadataContent: Boolean,
                            thumbnailsEnabled: Boolean,
                            smallThumbnailsSize: Option[ThumbnailSize],
                            mediumThumbnailsSize: Option[ThumbnailSize]) {

}

object ProcessingConfig {

  val AllowedDirectoriesKey = "filter.allowedMetadataDirectories"
  val AllowedTagsKey = "filter.allowedTags"
  val AllowedExtensionsKey = "filter.allowedExtensions"
  val IncludeDirectoriesInTagNamesKey = "output.columns.includeDirsInTags"
  val MetadataColumnsPrefixKey = "output.columns.metadataPrefix"
  val ColumnsNamingConventionKey = "output.columns.namingConvention"
  val IncludeMetadataContentKey = "output.columns.includeMetadataContent"
  val MandatoryTagsKey = "filter.mandatoryTags"
  val ThumbnailsEnabledKey = "processing.thumbnails.enabled"
  val ThumbnailsSmallSizeKey = "processing.thumbnails.smallDimensions"
  val ThumbnailsMediumSizeKey = "processing.thumbnails.mediumDimensions"
  val HashListKey = "processing.hash.types"
  val ProcessHashComparatorKey = "processing.hash.comparator"

  import ConfigLoader._

  val SizeDelimiter = "x"

  val Defaults = Map(
    AllowedDirectoriesKey -> All,
    AllowedTagsKey -> All,
    AllowedExtensionsKey -> All,
    ColumnsNamingConventionKey -> "camelCase",
    MetadataColumnsPrefixKey -> "",
    IncludeDirectoriesInTagNamesKey -> true,
    IncludeMetadataContentKey -> false,
    MandatoryTagsKey -> All,
    ThumbnailsEnabledKey -> false,
    ThumbnailsSmallSizeKey -> "",
    ThumbnailsMediumSizeKey -> "",
    HashListKey -> All,
    ProcessHashComparatorKey -> false
  )

  def build(config: Config): ProcessingConfig = {
    val configWithDefaults = assignDefaults(config, Defaults)
    val thumbnailsInfo = parseThumbnailsInfo(configWithDefaults)
    ProcessingConfig(
      configWithDefaults.getString(AllowedDirectoriesKey),
      configWithDefaults.getString(AllowedTagsKey),
      configWithDefaults.getString(MandatoryTagsKey),
      configWithDefaults.getString(HashListKey),
      configWithDefaults.getString(AllowedExtensionsKey),
      configWithDefaults.getBoolean(ProcessHashComparatorKey),
      configWithDefaults.getString(ColumnsNamingConventionKey),
      configWithDefaults.getString(MetadataColumnsPrefixKey),
      configWithDefaults.getBoolean(IncludeDirectoriesInTagNamesKey),
      configWithDefaults.getString(Writer.OutputFormatKey),
      configWithDefaults.getBoolean(IncludeMetadataContentKey),
      thumbnailsInfo._1,
      thumbnailsInfo._2,
      thumbnailsInfo._3
    )
  }

  protected def parseThumbnailsInfo(config: Config): (Boolean, Option[ThumbnailSize], Option[ThumbnailSize]) = {
    val enabled = config.getBoolean(ThumbnailsEnabledKey)
    if (enabled) {
      val small = parseThumbnailSize(config, ThumbnailsSmallSizeKey)
      val medium = parseThumbnailSize(config, ThumbnailsMediumSizeKey)
      (true, small, medium)
    } else {
      (false, None, None)
    }
  }

  protected def parseThumbnailSize(config: Config, key: String): Option[ThumbnailSize] = {
    val size = config.getString(key)
    if (size.isEmpty) {
      None
    } else {
      val parsedSize = size.toLowerCase.split(SizeDelimiter)
      if (parsedSize.size != 2) throw new RuntimeException(s"Wrong format of $key property's value. It should be: <INT_NUMBER>x<INT_NUMBER>, e.g.: 800x600 but is: $size")
      Some(ThumbnailSize(parsedSize(0).toInt, parsedSize(1).toInt))
    }
  }


}