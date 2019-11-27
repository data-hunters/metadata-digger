package ai.datahunters.md.config.processing

import ai.datahunters.md.UnitSpec
import ai.datahunters.md.config.Writer
import com.typesafe.config.ConfigFactory
import scala.collection.JavaConversions.mapAsJavaMap

class ProcessingConfigSpec extends UnitSpec {

  "A ProcessingConfig" should "create default CommonProcessingConfig for CSV" in {
    val inputConfig = Map(
      Writer.OutputFormatKey -> "json"
    )
    val config = ConfigFactory.parseMap(inputConfig)
    val outputConfig = ProcessingConfig.build(config).asInstanceOf[ProcessingConfig]
    assert(outputConfig.metadataColumnsPrefix === "")
    assert(outputConfig.outputFormat === "json")
    assert(outputConfig.namingConvention == "camelCase")
    assert(outputConfig.allowedTags === None)
    assert(outputConfig.includeDirsInTags === true)
    assert(outputConfig.mandatoryTags === None)
  }

  it should "create default CommontProcessingConfig for CSV" in {
    val inputConfig = Map(
      Writer.OutputFormatKey -> "csv"
    )
    val config = ConfigFactory.parseMap(inputConfig)
    val outputConfig = ProcessingConfig.build(config).asInstanceOf[ProcessingConfig]
    assert(outputConfig.metadataColumnsPrefix === "")
    assert(outputConfig.outputFormat === "csv")
    assert(outputConfig.namingConvention == "camelCase")
    assert(outputConfig.allowedTags === None)
    assert(outputConfig.includeDirsInTags === true)
    assert(outputConfig.mandatoryTags === None)
  }

  it should "create default SolrProcessingConfig" in {
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
  }
}
