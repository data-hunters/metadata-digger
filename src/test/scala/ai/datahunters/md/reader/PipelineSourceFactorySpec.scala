package ai.datahunters.md.reader

import ai.datahunters.md.config.reader.{LocalFSReaderConfig, ReaderConfig}
import ai.datahunters.md.{SparkBaseSpec, UnitSpec}

class PipelineSourceFactorySpec extends UnitSpec with SparkBaseSpec {

  "A PipelineSourceFactory" should "create BasicBinaryFilesReader" in {
    val readerConfig = mock[LocalFSReaderConfig]
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
