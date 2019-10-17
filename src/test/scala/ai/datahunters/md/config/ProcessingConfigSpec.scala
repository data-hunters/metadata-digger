package ai.datahunters.md.config

import ai.datahunters.md.UnitSpec
import com.typesafe.config.ConfigFactory

import scala.collection.JavaConversions.mapAsJavaMap

class ProcessingConfigSpec extends UnitSpec {

  "A ProcessingConfig" should "load default values" in {
    val inputConfig = Map[String, AnyVal]()
    val config = ConfigFactory.parseMap(inputConfig)
    val outputConfig = ProcessingConfig.build(config)
    assert(outputConfig.includeDirsInTags === false)
    assert(outputConfig.metadataColumnsPrefix === "")
    assert(outputConfig.namingConvention === "camelCase")
    assert(outputConfig.allowedDirectories === None)
    assert(outputConfig.allowedTags === None)
  }
}
