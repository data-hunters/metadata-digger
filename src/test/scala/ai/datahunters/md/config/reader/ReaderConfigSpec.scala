package ai.datahunters.md.config.reader

import ai.datahunters.md.UnitSpec
import ai.datahunters.md.config.GeneralConfig.NotSupportedStorageException
import ai.datahunters.md.config.{S3, reader}
import com.typesafe.config.ConfigFactory

import scala.collection.JavaConversions.mapAsJavaMap

class ReaderConfigSpec extends UnitSpec {

  "A ReaderConfig" should "create default config" in {
    val inputConfig = Map(
      FilesReaderConfig.InputPathsKey -> "/some/path"
    )
    val config = ConfigFactory.parseMap(inputConfig)
    val outputConfig = ReaderConfig(config).asInstanceOf[FilesReaderConfig]
    assert(outputConfig.inputPaths === Array("file:///some/path"))
    assert(outputConfig.partitionsNum === -1)
  }

  it should "create HDFS config" in {
    val inputConfig = Map(
      FilesReaderConfig.InputPathsKey -> "/some/path",
      ReaderConfig.StorageNameKey -> HDFSReaderConfig.StorageName
    )
    val config = ConfigFactory.parseMap(inputConfig)
    val outputConfig = ReaderConfig(config).asInstanceOf[HDFSReaderConfig]
    assert(outputConfig.inputPaths === Array("hdfs:///some/path"))
    assert(outputConfig.partitionsNum === -1)
  }

  it should "create S3 config" in {
    val inputConfig = Map(
      FilesReaderConfig.InputPathsKey -> "/some/path",
      ReaderConfig.StorageNameKey -> S3ReaderConfig.StorageName,
      S3.CredentialsProvidedInConfigKey -> false
    )
    val config = ConfigFactory.parseMap(inputConfig)
    val outputConfig = ReaderConfig(config).asInstanceOf[S3ReaderConfig]
    assert(outputConfig.inputPaths === Array("s3a://some/path"))
    assert(outputConfig.partitionsNum === -1)
  }

  it should "throw NotSupportedStorageException" in {
    val inputConfig = Map(
      FilesReaderConfig.InputPathsKey -> "/some/path",
      ReaderConfig.StorageNameKey -> "invalid_storage",
      S3.CredentialsProvidedInConfigKey -> false
    )
    val config = ConfigFactory.parseMap(inputConfig)
    intercept[NotSupportedStorageException] {
      ReaderConfig(config)
    }
  }
}
