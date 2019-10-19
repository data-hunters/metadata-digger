package ai.datahunters.md.config

import ai.datahunters.md.UnitSpec
import com.typesafe.config.ConfigFactory
import scala.collection.JavaConversions.mapAsJavaMap

class FilesReaderConfigSpec extends UnitSpec {

  "A FileReaderConfig" should "load default values" in {
    val inputConfig = Map(
      FilesReaderConfig.InputPathsKey -> "/some/path"
    )
    val config = ConfigFactory.parseMap(inputConfig)
    val outputConfig = FilesReaderConfig.build(config)
    assert(outputConfig.inputPaths === Array("/some/path"))
    assert(outputConfig.partitionsNum === -1)
  }
}
