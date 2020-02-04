package ai.datahunters.md.config.enrich

import ai.datahunters.md.UnitSpec
import ai.datahunters.md.config.Writer
import com.typesafe.config.ConfigFactory
import scala.collection.JavaConversions.mapAsJavaMap

class MetadataEnrichmentConfigSpec extends UnitSpec {

  "A MetadataEnrichmentConfig" should "create default MetadataEnrichmentConfig" in {
    val inputConfig = Map(
      MetadataEnrichmentConfig.LabelsMappingKey -> "0:person,1:car,2:truck",
      MetadataEnrichmentConfig.ModelPathKey -> "some_path"
    )
    val config = ConfigFactory.parseMap(inputConfig)
    val outputConfig = MetadataEnrichmentConfig.build(config)
    assert(outputConfig.labelsOutputDelimiter === ",")
    assert((outputConfig.threshold * 10).toInt === 5)
    assert(outputConfig.labelsMapping === Map(0 -> "person", 1 -> "car", 2 -> "truck"))
    assert(outputConfig.modelPath === "some_path")
  }

  it should "create MetadataEnrichmentConfig with custom values" in {
    val inputConfig = Map(
      MetadataEnrichmentConfig.LabelsMappingKey -> "0:person,1:car,2:truck",
      MetadataEnrichmentConfig.ModelPathKey -> "some_path",
      MetadataEnrichmentConfig.OutputLabelsDelimiterKey -> "|",
      MetadataEnrichmentConfig.PredictionThresholdKey -> 0.1f
    )
    val config = ConfigFactory.parseMap(inputConfig)
    val outputConfig = MetadataEnrichmentConfig.build(config)
    assert(outputConfig.labelsOutputDelimiter === "|")
    assert((outputConfig.threshold * 10).toInt === 1)
    assert(outputConfig.labelsMapping === Map(0 -> "person", 1 -> "car", 2 -> "truck"))
    assert(outputConfig.modelPath === "some_path")
  }

}
