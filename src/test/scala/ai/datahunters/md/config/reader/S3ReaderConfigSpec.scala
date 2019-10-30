package ai.datahunters.md.config.reader

import ai.datahunters.md.UnitSpec
import ai.datahunters.md.config.S3
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql

import scala.collection.JavaConversions.mapAsJavaMap

class S3ReaderConfigSpec extends UnitSpec {

  import S3._
  import ai.datahunters.md.config.S3Samples._
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
      EndpointKey -> SampleEndpoint,
      AccessKeyKey -> SampleAccessKey,
      SecretKeyKey -> SampleSecretKey
    )
    val config = ConfigFactory.parseMap(inputConfig)
    val outputConfig = S3ReaderConfig.build(config)
    assert(outputConfig.inputPaths === Array(SamplePath1, s"${PathPrefix}one/another/path", SamplePath2, s"${PathPrefix}almost/correct/path"))
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
    assert(confBefore.get(HadoopS3SecretKeyKey) === null)
    assert(confBefore.get(HadoopFsS3ImplKey) === null)
    assert(confBefore.get(HadoopS3EndpointKey) === null)
    assert(confBefore.get(HadoopS3CredsProviderKey) === null)
    assert(confBefore.get(HadoopS3AccessKeyKey) === null)
    assert(confBefore.get(HadoopS3PathStyleAccessKey) === "false")
    outputConfig.adjustSparkConfig(sparkSession)
    val confAfter = sparkSession.sparkContext.hadoopConfiguration
    assert(confAfter.get(HadoopS3SecretKeyKey) === SampleSecretKey)
    assert(confBefore.get(HadoopFsS3ImplKey) === HadoopFsS3Impl)
    assert(confBefore.get(HadoopS3EndpointKey) === SampleEndpoint)
    assert(confBefore.get(HadoopS3CredsProviderKey) === HadoopS3CredsProvider)
    assert(confBefore.get(HadoopS3AccessKeyKey) === SampleAccessKey)
    assert(confBefore.get(HadoopS3PathStyleAccessKey) === HadoopS3PathStyleAccess)
  }
}

object S3ReaderConfigSpec {

  import ai.datahunters.md.config.S3Samples._

  val SampleDefaults = Map(
    FilesReaderConfig.InputPathsKey -> s"$SamplePath1,$SamplePath2",
    S3.EndpointKey -> SampleEndpoint,
    S3.AccessKeyKey -> SampleAccessKey,
    S3.SecretKeyKey -> SampleSecretKey
  )
}
