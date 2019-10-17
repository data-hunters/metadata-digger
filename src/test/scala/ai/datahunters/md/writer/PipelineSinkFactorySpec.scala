package ai.datahunters.md.writer

import ai.datahunters.md.{SparkBaseSpec, UnitSpec}
import ai.datahunters.md.config.{FilesWriterConfig, SolrWriterConfig, WriterConfig}
import com.typesafe.config.ConfigFactory
import org.mockito.Mockito._

class PipelineSinkFactorySpec extends UnitSpec with SparkBaseSpec {
  import scala.collection.JavaConversions.mapAsJavaMap

  "A PipelineSinkFactory" should "create BasicFileOutputWriter for json format" in {
    val configMap = Map(
      FilesWriterConfig.OutputFilesNumKey -> 2,
      FilesWriterConfig.OutputDirPathKey -> "some/path",
      WriterConfig.OutputFormatKey -> "json"
    )
    val config = ConfigFactory.parseMap(configMap)
    val sink = PipelineSinkFactory.create(config, sparkSession)
    assert(sink.isInstanceOf[BasicFileOutputWriter])
  }

  it should "create BasicFileOutputWriter for csv format" in {
    val configMap = Map(
      FilesWriterConfig.OutputFilesNumKey -> 1,
      FilesWriterConfig.OutputDirPathKey -> "some/path2",
      WriterConfig.OutputFormatKey -> "csv"
    )
    val config = ConfigFactory.parseMap(configMap)
    val sink = PipelineSinkFactory.create(config, sparkSession)
    assert(sink.isInstanceOf[BasicFileOutputWriter])
  }

  it should "create SolrWriter for solr format" in {
    val configMap = Map(
      WriterConfig.OutputFormatKey -> SolrWriter.FormatName,
      SolrWriterConfig.CollectionKey -> "col1",
      SolrWriterConfig.ZKServersKey -> "localhost:2181"
    )
    val config = ConfigFactory.parseMap(configMap)
    val sink = PipelineSinkFactory.create(config, sparkSession)
    assert(sink.isInstanceOf[SolrWriter])
  }
}
