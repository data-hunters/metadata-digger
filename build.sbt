import sbt.Provided

name := "metadata-digger"

version := "0.1.1"

scalaVersion := "2.11.12"

val sparkV = "2.4.3"
val metadataExtractorV = "2.12.0"
val scalaTestV = "3.0.8"
val mockitoV = "3.1.0"
val typesafeConfigV = "1.3.4"
val solrV = "8.2.0"



lazy val sparkDependencies = Seq(
  "org.apache.spark" %% "spark-sql" % sparkV % Provided,
  "org.apache.spark" %% "spark-mllib" % sparkV % Provided
)

lazy val externalConnectors = Seq(
  "org.apache.solr" % "solr-solrj" % solrV
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
assemblyJarName in assembly := s"${name.value}-${version.value}.jar"

//resolvers += "maven.restlet.org" at "http://maven.restlet.org"

testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-y", "org.scalatest.FlatSpec")
logBuffered in Test := false
parallelExecution in Test := false