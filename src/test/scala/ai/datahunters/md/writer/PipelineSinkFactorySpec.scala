package ai.datahunters.md.writer

import ai.datahunters.md.config.writer.{FilesWriterConfig, LocalFSWriterConfig, SolrWriterConfig, WriterConfig}
import ai.datahunters.md.{SparkBaseSpec, UnitSpec}
import com.typesafe.config.ConfigFactory

class PipelineSinkFactorySpec extends UnitSpec with SparkBaseSpec {
  import scala.collection.JavaConversions.mapAsJavaMap
  import ai.datahunters.md.config.Writer._

  "A PipelineSinkFactory" should "create BasicFileOutputWriter for json format" in {
    val configMap = Map(
      FilesWriterConfig.OutputFilesNumKey -> 2,
      FilesWriterConfig.OutputDirPathKey -> "some/path",
      OutputFormatKey -> "json",
      StorageNameKey -> LocalFSWriterConfig.StorageName
    )
    val config = ConfigFactory.parseMap(configMap)
    val sink = PipelineSinkFactory.create(WriterConfig(config), sparkSession)
    assert(sink.isInstanceOf[BasicFileOutputWriter])
  }

  it should "create BasicFileOutputWriter for csv format" in {
    val configMap = Map(
      FilesWriterConfig.OutputFilesNumKey -> 1,
      FilesWriterConfig.OutputDirPathKey -> "some/path2",
      OutputFormatKey -> "csv",
      StorageNameKey -> LocalFSWriterConfig.StorageName
    )
    val config = ConfigFactory.parseMap(configMap)
    val sink = PipelineSinkFactory.create(WriterConfig(config), sparkSession)
    assert(sink.isInstanceOf[BasicFileOutputWriter])
  }

  it should "create SolrWriter for solr format" in {
    val configMap = Map(
      OutputFormatKey -> SolrWriter.FormatName,
      SolrWriterConfig.CollectionKey -> "col1",
      SolrWriterConfig.ZKServersKey -> "localhost:2181",
      StorageNameKey -> SolrWriterConfig.StorageName
    )
    val config = ConfigFactory.parseMap(configMap)
    val sink = PipelineSinkFactory.create(WriterConfig(config), sparkSession)
    assert(sink.isInstanceOf[SolrWriter])
  }
}
