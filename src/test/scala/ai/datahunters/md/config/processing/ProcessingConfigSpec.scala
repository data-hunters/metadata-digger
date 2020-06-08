package ai.datahunters.md.config.processing

import ai.datahunters.md.UnitSpec
import ai.datahunters.md.config.Writer
import ai.datahunters.md.udf.image.ImageProcessing.ThumbnailSize
import com.typesafe.config.ConfigFactory

import scala.collection.JavaConversions.mapAsJavaMap

class ProcessingConfigSpec extends UnitSpec {

  "A ProcessingConfig" should "create default ProcessingConfig for JSON" in {
    val inputConfig = Map(
      Writer.OutputFormatKey -> "json"
    )
    val config = ConfigFactory.parseMap(inputConfig)
    val outputConfig = ProcessingConfig.build(config)
    assert(outputConfig.metadataColumnsPrefix === "")
    assert(outputConfig.outputFormat === "json")
    assert(outputConfig.namingConvention == "camelCase")
    assert(outputConfig.allowedTags === None)
    assert(outputConfig.includeDirsInTags === true)
    assert(outputConfig.mandatoryTags === None)
    assert(outputConfig.thumbnailsEnabled === false)
    assert(outputConfig.allowedExtensions === None)
  }

  it should "create default ProcessingConfig for CSV" in {
    val inputConfig = Map(
      Writer.OutputFormatKey -> "csv"
    )
    val config = ConfigFactory.parseMap(inputConfig)
    val outputConfig = ProcessingConfig.build(config)
    assert(outputConfig.metadataColumnsPrefix === "")
    assert(outputConfig.outputFormat === "csv")
    assert(outputConfig.namingConvention == "camelCase")
    assert(outputConfig.allowedTags === None)
    assert(outputConfig.includeDirsInTags === true)
    assert(outputConfig.mandatoryTags === None)
    assert(outputConfig.thumbnailsEnabled === false)
    assert(outputConfig.allowedExtensions === None)
  }

  it should "create default ProcessingConfig for Solr" in {
    val inputConfig = Map(
      Writer.OutputFormatKey -> "solr"
    )
    val config = ConfigFactory.parseMap(inputConfig)
    val outputConfig = ProcessingConfig.build(config)
    assert(outputConfig.metadataColumnsPrefix === "")
    assert(outputConfig.outputFormat === "solr")
    assert(outputConfig.namingConvention == "camelCase")
    assert(outputConfig.allowedTags === None)
    assert(outputConfig.includeDirsInTags === true)
    assert(outputConfig.mandatoryTags === None)
    assert(outputConfig.thumbnailsEnabled === false)
    assert(outputConfig.allowedExtensions === None)
  }

  it should "create ProcessingConfig with small thumbnails size" in {
    val inputConfig = Map(
      Writer.OutputFormatKey -> "json",
      ProcessingConfig.ThumbnailsEnabledKey -> true,
      ProcessingConfig.ThumbnailsSmallSizeKey -> "200x300"
    )
    val config = ConfigFactory.parseMap(inputConfig)
    val outputConfig = ProcessingConfig.build(config)
    assert(outputConfig.thumbnailsEnabled === true)
    assert(outputConfig.smallThumbnailsSize.get === ThumbnailSize(200, 300))
    assert(outputConfig.mediumThumbnailsSize === None)
  }

  it should "create ProcessingConfig with small and medium thumbnails sizes" in {
    val inputConfig = Map(
      Writer.OutputFormatKey -> "json",
      ProcessingConfig.ThumbnailsEnabledKey -> true,
      ProcessingConfig.ThumbnailsSmallSizeKey -> "200x300",
      ProcessingConfig.ThumbnailsMediumSizeKey -> "500x400"
    )
    val config = ConfigFactory.parseMap(inputConfig)
    val outputConfig = ProcessingConfig.build(config)
    assert(outputConfig.thumbnailsEnabled === true)
    assert(outputConfig.smallThumbnailsSize.get === ThumbnailSize(200, 300))
    assert(outputConfig.mediumThumbnailsSize.get === ThumbnailSize(500, 400))
  }
}
