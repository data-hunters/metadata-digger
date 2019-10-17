package ai.datahunters.md.reader

import ai.datahunters.md.{SparkBaseSpec, UnitSpec}
import ai.datahunters.md.config.{FilesReaderConfig, ReaderConfig}

class PipelineSourceFactorySpec extends UnitSpec with SparkBaseSpec {

  "A PipelineSourceFactory" should "create BasicBinaryFilesReader" in {
    val readerConfig = mock[FilesReaderConfig]
    val reader = PipelineSourceFactory.create(readerConfig, sparkSession)
    assert(reader.isInstanceOf[BasicBinaryFilesReader])
  }

  it should "throw exception for not supported type of config" in {
    val readerConfig = mock[ReaderConfig]
    intercept[RuntimeException] {
      PipelineSourceFactory.create(readerConfig, sparkSession)
    }
  }

}
