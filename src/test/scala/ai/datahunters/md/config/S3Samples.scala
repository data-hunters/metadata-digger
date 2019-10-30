package ai.datahunters.md.config

import ai.datahunters.md.config.writer.FilesWriterConfig

object S3Samples {

  val SamplePath1 = "s3a://some/path"
  val SamplePath2 = "s3a://another/path"
  val SampleInvalidPath1 = "/one/another/path"
  val SampleInvalidPath2 = "s3a:///almost/correct/path"
  val SampleEndpoint = "https://region.hostname.com"
  val SampleAccessKey = "someKey"
  val SampleSecretKey = "secret"



}
