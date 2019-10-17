package ai.datahunters.md.config

import ai.datahunters.md.UnitSpec
import com.typesafe.config.ConfigFactory
import scala.collection.JavaConversions.mapAsJavaMap

class FilesWriterConfigSpec extends UnitSpec {

  "A FilesWriterConfig" should "load default values" in {
    val inputConfig = Map(
      FilesWriterConfig.OutputDirPathKey -> "/some/path",
      WriterConfig.OutputFormatKey -> "json"
    )
    val config = ConfigFactory.parseMap(inputConfig)
    val outputConfig = FilesWriterConfig.build(config)
    assert(outputConfig.outputDirPath === "/some/path")
    assert(outputConfig.outputFilesNum === 1)
    assert(outputConfig.format === "json")
  }
}
