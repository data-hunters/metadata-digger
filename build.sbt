
import ai.datahunters.sbtrelease.ReleaseTasks
import ai.datahunters.sbtrelease.ReleaseTasks._
import sbt.Provided

name := "metadata-digger"

version := "0.1.2"

scalaVersion := "2.11.12"

organization := "ai.datahunters"

organizationHomepage := Some(url("http://datahunters.ai"))


val sparkV = "2.4.3"
val metadataExtractorV = "2.12.0"
val scalaTestV = "3.0.8"
val mockitoV = "3.1.0"
val typesafeConfigV = "1.3.4"
val solrV = "8.2.0"
val hadoopV = "2.8.0" // This variable is not used to force Hadoop dependencies from Spark packages
val hadoopAWSV = hadoopV
val awsSDKV = "1.11.656"
val zooV = "0.6.0"
val imageIOV = "3.2.1"
val commonsLangV = "3.5"

lazy val sparkDependencies = Seq(
  "org.apache.spark" %% "spark-sql" % sparkV % Provided,
  "org.apache.spark" %% "spark-mllib" % sparkV % Provided
)

lazy val analyticsDependencies = Seq(
  "com.intel.analytics.zoo" % "analytics-zoo-bigdl_0.9.1-spark_2.4.3" % zooV 
    exclude("org.tensorflow", "*") 
    exclude("org.apache.commons", "commons-lang3")
    exclude("log4j", "log4j")
    exclude("org.slf4j", "*")
)

lazy val externalConnectors = Seq(
  "org.apache.solr" % "solr-solrj" % solrV exclude("org.slf4j", "*"),
  "org.apache.hadoop" % "hadoop-aws" % hadoopAWSV % Provided exclude("com.fasterxml.jackson.core", "*") exclude("com.amazonaws", "*"),
  "com.amazonaws" % "aws-java-sdk" % awsSDKV % Provided
)

lazy val utilsDependencies = Seq(
  "com.typesafe" % "config" % typesafeConfigV,
  "com.drewnoakes" % "metadata-extractor" % metadataExtractorV,
  "com.twelvemonkeys.imageio" % "imageio-core" % imageIOV,
  "com.twelvemonkeys.imageio" % "imageio-tiff" % imageIOV//,
//  "org.apache.commons" % "commons-lang3" % commonsLangV // Forcing this version to avoid issue "Illegal pattern component: XXX"
)

lazy val testDependencies = Seq(
  "org.scalatest" %% "scalatest" % scalaTestV % Test,
  "org.mockito" % "mockito-core" % mockitoV % Test
)

libraryDependencies ++= (sparkDependencies) ++ (externalConnectors) ++ (utilsDependencies) ++ (testDependencies) ++ (analyticsDependencies)

assemblyMergeStrategy in assembly := {
    case PathList("reference.conf") => MergeStrategy.concat
    case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
    case _ => MergeStrategy.first
}


assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

assemblyOutputPath in assembly := target.value / s"${name.value}-${version.value}.jar"


lazy val root = project.in(file("."))
  .configs(DistStandaloneConfig, AWSDepsConfig)
  .settings(
    ReleaseTasks.build(assembly),
    inConfig(AWSDepsConfig)(Classpaths.ivyBaseSettings),
    inConfig(DistStandaloneConfig)(Classpaths.ivyBaseSettings)
  )

libraryDependencies in DistStandaloneConfig := Seq(
  "org.apache.spark" %% "spark-sql" % sparkV,
  "org.apache.spark" %% "spark-mllib" % sparkV,
  "org.scala-lang" % "scala-compiler" % scalaVersion.value,
  "org.scala-lang" % "scala-library" % scalaVersion.value,
  "org.scala-lang" % "scala-reflect" % scalaVersion.value
)

libraryDependencies in AWSDepsConfig := Seq(
  "org.apache.hadoop" % "hadoop-aws" % hadoopAWSV exclude("com.fasterxml.jackson.core", "*") exclude("com.amazonaws", "*"),
  "com.amazonaws" % "aws-java-sdk" % awsSDKV
)


// Configuration for Tests
testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-y", "org.scalatest.FlatSpec")
logBuffered in Test := false
parallelExecution in Test := false
