package ai.datahunters.md.config.writer

import ai.datahunters.md.UnitSpec
import ai.datahunters.md.config.{S3, Writer}
import ai.datahunters.md.config.S3._
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql

import scala.collection.JavaConversions.mapAsJavaMap

class S3WriterConfigSpec extends UnitSpec {

  import ai.datahunters.md.config.S3Samples._
  import S3WriterConfigSpec._

  "A S3ReaderConfig" should "load default values" in {
    val inputConfig = SampleDefaults
    val config = ConfigFactory.parseMap(inputConfig)
    val outputConfig = S3WriterConfig.build(config)
    assert(outputConfig.outputDirPath === SamplePath1)
    assert(outputConfig.secretKey === SampleSecretKey)
    assert(outputConfig.accessKey === SampleAccessKey)
    assert(outputConfig.endpoint.get === SampleEndpoint)
    assert(outputConfig.credentialsProvidedInConfig === true)
    assert(outputConfig.outputFilesNum === 1)
    assert(outputConfig.format === "json")
  }

  it should "load default values and fix wrong paths" in {
    val inputConfig = Map(
      FilesWriterConfig.OutputDirPathKey -> SampleInvalidPath1,
      EndpointKey -> SampleEndpoint,
      AccessKeyKey -> SampleAccessKey,
      SecretKeyKey -> SampleSecretKey,
      Writer.OutputFormatKey -> "json"
    )
    val config = ConfigFactory.parseMap(inputConfig)
    val outputConfig = S3WriterConfig.build(config)
    assert(outputConfig.outputDirPath === s"${PathPrefix}one/another/path")
    assert(outputConfig.secretKey === SampleSecretKey)
    assert(outputConfig.accessKey === SampleAccessKey)
    assert(outputConfig.endpoint.get === SampleEndpoint)
    assert(outputConfig.credentialsProvidedInConfig === true)
    assert(outputConfig.outputFilesNum === 1)
    assert(outputConfig.format === "json")
  }

  it should "adjust SparkContext Hadoop configuration" in {
    val sparkSession = new sql.SparkSession.Builder()
      .master("local[1]")
      .appName("test")
      .getOrCreate()
    val inputConfig = SampleDefaults
    val config = ConfigFactory.parseMap(inputConfig)
    val outputConfig = S3WriterConfig.build(config)
    val confBefore = sparkSession.sparkContext.hadoopConfiguration
    assert(confBefore.get(HadoopS3SecretKeyKey) === null)
    assert(confBefore.get(HadoopFsS3ImplKey) === null)
    assert(confBefore.get(HadoopS3EndpointKey) === null)
    assert(confBefore.get(HadoopS3CredsProviderKey) === null)
    assert(confBefore.get(HadoopS3AccessKeyKey) === null)
    assert(confBefore.get(HadoopS3PathStyleAccessKey) === "false")
    val confAfter = sparkSession.sparkContext.hadoopConfiguration
    outputConfig.adjustSparkConfig(sparkSession)
    assert(confAfter.get(HadoopS3SecretKeyKey) === SampleSecretKey)
    assert(confAfter.get(HadoopFsS3ImplKey) === HadoopFsS3Impl)
    assert(confAfter.get(HadoopS3EndpointKey) === SampleEndpoint)
    assert(confAfter.get(HadoopS3CredsProviderKey) === HadoopS3CredsProvider)
    assert(confAfter.get(HadoopS3AccessKeyKey) === SampleAccessKey)
    assert(confAfter.get(HadoopS3PathStyleAccessKey) === HadoopS3PathStyleAccess)
    sparkSession.close()
  }
}

object S3WriterConfigSpec {

  import ai.datahunters.md.config.S3Samples._

  val SampleDefaults = Map(
    FilesWriterConfig.OutputDirPathKey -> SamplePath1,
    Writer.OutputFormatKey -> "json",
    S3.EndpointKey -> SampleEndpoint,
    S3.AccessKeyKey -> SampleAccessKey,
    S3.SecretKeyKey -> SampleSecretKey
  )

}