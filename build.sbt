import sbt.Provided

name := "metadata-digger"

version := "0.1.1"

scalaVersion := "2.11.12"

val sparkV = "2.4.3"
val metadataExtractorV = "2.12.0"
val scalaTestV = "3.0.8"
val mockitoV = "3.1.0"
val typesafeConfigV = "1.3.4"
val standaloneProp = System.getProperty("standalone")



lazy val sparkDependencies = Seq(
  "org.apache.spark" %% "spark-sql" % sparkV % Provided,
  "org.apache.spark" %% "spark-mllib" % sparkV % Provided
)

lazy val utilsDependencies = Seq(
  "com.typesafe" % "config" % typesafeConfigV,
  "com.drewnoakes" % "metadata-extractor" % metadataExtractorV 
)

lazy val testDependencies = Seq(
  "org.scalatest" %% "scalatest" % scalaTestV % Test,
  "org.mockito" % "mockito-core" % mockitoV % Test 
)

libraryDependencies ++= (sparkDependencies) ++ (utilsDependencies) ++ (testDependencies)

assemblyMergeStrategy in assembly := {
    case PathList("reference.conf") => MergeStrategy.concat
    case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
    case _ => MergeStrategy.first
}



testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-y", "org.scalatest.FlatSpec")
logBuffered in Test := false
parallelExecution in Test := false