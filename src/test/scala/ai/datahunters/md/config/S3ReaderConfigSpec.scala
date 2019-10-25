package ai.datahunters.md.config

import ai.datahunters.md.UnitSpec
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConversions.mapAsJavaMap

class S3ReaderConfigSpec extends UnitSpec {

  import S3ReaderConfigSpec._

  "A S3ReaderConfig" should "load default values" in {
    val inputConfig = SampleDefaults
    val config = ConfigFactory.parseMap(inputConfig)
    val outputConfig = S3ReaderConfig.build(config)
    assert(outputConfig.inputPaths === Array(SamplePath1, SamplePath2))
    assert(outputConfig.secretKey === SampleSecretKey)
    assert(outputConfig.accessKey === SampleAccessKey)
    assert(outputConfig.endpoint.get === SampleEndpoint)
    assert(outputConfig.credentialsProvidedInConfig === true)
    assert(outputConfig.partitionsNum === -1)
  }

  it should "load default values and fix wrong paths" in {
    val inputConfig = Map(
      FilesReaderConfig.InputPathsKey -> s"$SamplePath1,$SampleInvalidPath1,$SamplePath2,$SampleInvalidPath2",
      S3ReaderConfig.EndpointKey -> SampleEndpoint,
      S3ReaderConfig.AccessKeyKey -> SampleAccessKey,
      S3ReaderConfig.SecretKeyKey -> SampleSecretKey
    )
    val config = ConfigFactory.parseMap(inputConfig)
    val outputConfig = S3ReaderConfig.build(config)
    assert(outputConfig.inputPaths === Array(SamplePath1, s"${S3ReaderConfig.PathPrefix}one/another/path", SamplePath2, s"${S3ReaderConfig.PathPrefix}almost/correct/path"))
    assert(outputConfig.secretKey === SampleSecretKey)
    assert(outputConfig.accessKey === SampleAccessKey)
    assert(outputConfig.endpoint.get === SampleEndpoint)
    assert(outputConfig.credentialsProvidedInConfig === true)
    assert(outputConfig.partitionsNum === -1)
  }

  it should "adjust SparkContext Hadoop configuration" in {
    val sparkSession = new sql.SparkSession.Builder()
      .master("local[1]")
      .appName("test")
      .getOrCreate()
    val inputConfig = SampleDefaults
    val config = ConfigFactory.parseMap(inputConfig)
    val outputConfig = S3ReaderConfig.build(config)
    val confBefore = sparkSession.sparkContext.hadoopConfiguration
    assert(confBefore.get(S3ReaderConfig.HadoopS3SecretKeyKey) === null)
    assert(confBefore.get(S3ReaderConfig.HadoopFsS3ImplKey) === null)
    assert(confBefore.get(S3ReaderConfig.HadoopS3EndpointKey) === null)
    assert(confBefore.get(S3ReaderConfig.HadoopS3CredsProviderKey) === null)
    assert(confBefore.get(S3ReaderConfig.HadoopS3AccessKeyKey) === null)
    assert(confBefore.get(S3ReaderConfig.HadoopS3PathStyleAccessKey) === "false")
    outputConfig.adjustSparkConfig(sparkSession)
    val confAfter = sparkSession.sparkContext.hadoopConfiguration
    assert(confAfter.get(S3ReaderConfig.HadoopS3SecretKeyKey) === SampleSecretKey)
    assert(confBefore.get(S3ReaderConfig.HadoopFsS3ImplKey) === S3ReaderConfig.HadoopFsS3Impl)
    assert(confBefore.get(S3ReaderConfig.HadoopS3EndpointKey) === SampleEndpoint)
    assert(confBefore.get(S3ReaderConfig.HadoopS3CredsProviderKey) === S3ReaderConfig.HadoopS3CredsProvider)
    assert(confBefore.get(S3ReaderConfig.HadoopS3AccessKeyKey) === SampleAccessKey)
    assert(confBefore.get(S3ReaderConfig.HadoopS3PathStyleAccessKey) === S3ReaderConfig.HadoopS3PathStyleAccess)
  }
}

object S3ReaderConfigSpec {

  val SamplePath1 = "s3a://some/path"
  val SamplePath2 = "s3a://another/path"
  val SampleInvalidPath1 = "/one/another/path"
  val SampleInvalidPath2 = "s3a:///almost/correct/path"
  val SampleEndpoint = "https://region.hostname.com"
  val SampleAccessKey = "someKey"
  val SampleSecretKey = "secret"

  val SampleDefaults = Map(
    FilesReaderConfig.InputPathsKey -> s"$SamplePath1,$SamplePath2",
    S3ReaderConfig.EndpointKey -> SampleEndpoint,
    S3ReaderConfig.AccessKeyKey -> SampleAccessKey,
    S3ReaderConfig.SecretKeyKey -> SampleSecretKey
  )
}
