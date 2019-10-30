package ai.datahunters.md.config.writer

import ai.datahunters.md.UnitSpec
import com.typesafe.config.ConfigFactory
import scala.collection.JavaConversions.mapAsJavaMap

class WriterConfigSpec extends UnitSpec {
  import ai.datahunters.md.config.Writer._

  "A WriterConfig" should "create default config" in {
    val inputConfig = Map(
      FilesWriterConfig.OutputDirPathKey -> "/some/path",
      OutputFormatKey -> "json"
    )
    val config = ConfigFactory.parseMap(inputConfig)
    val outputConfig = WriterConfig.build(config).asInstanceOf[FilesWriterConfig]
    assert(outputConfig.outputDirPath === "file:///some/path")
    assert(outputConfig.format === "json")
    assert(outputConfig.outputFilesNum === 1)
  }

  it should "create HDFS config" in {
    val inputConfig = Map(
      FilesWriterConfig.OutputDirPathKey -> "/some/path",
      OutputFormatKey -> "json",
      StorageNameKey -> HDFSWriterConfig.StorageName
    )
    val config = ConfigFactory.parseMap(inputConfig)
    val outputConfig = WriterConfig.build(config).asInstanceOf[FilesWriterConfig]
    assert(outputConfig.outputDirPath === "hdfs:///some/path")
    assert(outputConfig.format === "json")
    assert(outputConfig.outputFilesNum === 1)
  }

  it should "create Solr config" in {
    val inputConfig = Map(
      FilesWriterConfig.OutputDirPathKey -> "/some/path",
      StorageNameKey -> SolrWriterConfig.StorageName,
      SolrWriterConfig.CollectionKey -> "col1",
      SolrWriterConfig.ZKServersKey -> "localhost:2181"
    )
    val config = ConfigFactory.parseMap(inputConfig)
    val outputConfig = WriterConfig.build(config).asInstanceOf[SolrWriterConfig]
    assert(outputConfig.collection === "col1")
    assert(outputConfig.zkServers === Seq("localhost:2181"))
    assert(outputConfig.zkSolrZNode === None)
  }
}
