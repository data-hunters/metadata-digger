package ai.datahunters.md.config.writer

import ai.datahunters.md.UnitSpec
import com.typesafe.config.ConfigFactory
import scala.collection.JavaConversions.mapAsJavaMap

class HDFSWriterConfigSpec extends UnitSpec {

  import ai.datahunters.md.config.Writer._

  "A HDFSWriterConfig" should "load default values" in {
    val inputConfig = Map(
      FilesWriterConfig.OutputDirPathKey -> "/some/path",
      OutputFormatKey -> "json",
      StorageNameKey -> HDFSWriterConfig.StorageName
    )
    val config = ConfigFactory.parseMap(inputConfig)
    val outputConfig = HDFSWriterConfig.build(config)
    assert(outputConfig.outputDirPath === "hdfs:///some/path")
    assert(outputConfig.outputFilesNum === 1)
    assert(outputConfig.format === "json")
  }
}
