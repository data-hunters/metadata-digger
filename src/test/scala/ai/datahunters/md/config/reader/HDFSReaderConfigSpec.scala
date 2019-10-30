package ai.datahunters.md.config.reader

import ai.datahunters.md.UnitSpec
import com.typesafe.config.ConfigFactory

import scala.collection.JavaConversions.mapAsJavaMap

class HDFSReaderConfigSpec extends UnitSpec {

  "A HDFSReaderConfig" should "load default values" in {
    val inputConfig = Map(
      FilesReaderConfig.InputPathsKey -> "/some/path"
    )
    val config = ConfigFactory.parseMap(inputConfig)
    val outputConfig = HDFSReaderConfig.build(config)
    assert(outputConfig.inputPaths === Array("hdfs:///some/path"))
    assert(outputConfig.partitionsNum === -1)
  }
}
