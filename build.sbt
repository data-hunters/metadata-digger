import java.nio.file.Files

import sbt.Provided

name := "metadata-digger"

version := "0.1.1"

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


lazy val sparkDependencies = Seq(
  "org.apache.spark" %% "spark-sql" % sparkV % Provided,
  "org.apache.spark" %% "spark-mllib" % sparkV % Provided
)

lazy val externalConnectors = Seq(
  "org.apache.solr" % "solr-solrj" % solrV exclude("org.slf4j", "*"),
  "org.apache.hadoop" % "hadoop-aws" % hadoopAWSV % Provided exclude("com.fasterxml.jackson.core", "*") exclude("com.amazonaws", "*"),
  "com.amazonaws" % "aws-java-sdk" % awsSDKV % Provided
)

lazy val utilsDependencies = Seq(
  "com.typesafe" % "config" % typesafeConfigV,
  "com.drewnoakes" % "metadata-extractor" % metadataExtractorV 
)

lazy val testDependencies = Seq(
  "org.scalatest" %% "scalatest" % scalaTestV % Test,
  "org.mockito" % "mockito-core" % mockitoV % Test
)

libraryDependencies ++= (sparkDependencies) ++ (externalConnectors) ++ (utilsDependencies) ++ (testDependencies)

assemblyMergeStrategy in assembly := {
    case PathList("reference.conf") => MergeStrategy.concat
    case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
    case _ => MergeStrategy.first
}


assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

assemblyOutputPath in assembly := target.value / s"${name.value}-${version.value}.jar"


val distTargetDirName = "dist"
val distLibDirName = "lib"
val configsDirName = "configs"
val scriptsDirName = "scripts"

// Task creating dist for Standalone Mode

val awsLibsDirName = "aws_libs"
val standaloneDirName = "standalone"
lazy val distStandalone = taskKey[Unit]("Create release package for Standalone Mode")
lazy val DistStandaloneConfig = config("distStandalone")

libraryDependencies in DistStandaloneConfig := Seq(
  "com.amazonaws" % "aws-java-sdk" % awsSDKV,
  "org.apache.spark" %% "spark-sql" % sparkV,
  "org.apache.spark" %% "spark-mllib" % sparkV,
  "org.scala-lang" % "scala-compiler" % scalaVersion.value,
  "org.scala-lang" % "scala-library" % scalaVersion.value,
  "org.scala-lang" % "scala-reflect" % scalaVersion.value
)


distStandalone := {
  (update in DistStandaloneConfig).value.allFiles.foreach { f =>
    IO.copyFile(f, target.value / distTargetDirName / standaloneDirName / distLibDirName /  f.getName)
  }
  // Copying sample configs
  IO.copyDirectory(
    (Compile / resourceDirectory).value / configsDirName, 
    target.value / distTargetDirName / standaloneDirName / configsDirName
  )
  // Copying running script
  IO.copyDirectory(
    (Compile / resourceDirectory).value / scriptsDirName / standaloneDirName, 
    target.value / distTargetDirName / standaloneDirName
  )
  // Copying main JAR
  IO.copyFile(
    target.value / s"${name.value}-${version.value}.jar",
    target.value / distTargetDirName / standaloneDirName / s"${name.value}-${version.value}.jar"
  )
}


// Task creating dist for Distributed Mode

val distributedDirName = "distributed"
lazy val distDistributed = taskKey[Unit]("Create release package for Distributed Mode")
lazy val DistDistributedConfig = config("distDistributed")


libraryDependencies in DistDistributedConfig := Seq(
  "com.amazonaws" % "aws-java-sdk" % awsSDKV
)


distDistributed := {
  (update in DistDistributedConfig).value.allFiles.foreach { f =>
    IO.copyFile(f, target.value / distTargetDirName / distributedDirName / awsLibsDirName /  f.getName)
  }
  // Copying sample configs
  IO.copyDirectory(
    (Compile / resourceDirectory).value / configsDirName, 
    target.value / distTargetDirName / distributedDirName / configsDirName
  )
  // Copying running script
  IO.copyDirectory(
    (Compile / resourceDirectory).value / scriptsDirName / distributedDirName, 
    target.value / distTargetDirName / distributedDirName
  )
  // Copying main JAR
  IO.copyFile(
    target.value / s"${name.value}-${version.value}.jar",
    target.value / distTargetDirName / distributedDirName / s"${name.value}-${version.value}.jar"
  )

}

lazy val root = project.in(file("."))
  .configs(DistStandaloneConfig, DistDistributedConfig)
  .settings(
    inConfig(DistDistributedConfig)(Classpaths.ivyBaseSettings),
    inConfig(DistStandaloneConfig)(Classpaths.ivyBaseSettings)
  )




// Bundle Task creating whole distributable package including Distributed and Standalone Mode

lazy val dist = taskKey[Unit]("Create distributable package")

dist := Def.sequential(assembly, distStandalone, distDistributed).value


// Configuration for Tests
testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-y", "org.scalatest.FlatSpec")
logBuffered in Test := false
parallelExecution in Test := false
