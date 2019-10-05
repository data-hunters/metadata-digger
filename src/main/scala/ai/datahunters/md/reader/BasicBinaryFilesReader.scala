package ai.datahunters.md.reader
import java.nio.file.{Path, Paths}

import ai.datahunters.md.schema.{BinaryInputSchemaConfig, SchemaConfig}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

case class BasicBinaryFilesReader(sparkSession: SparkSession, partitionsNum: Int, schemaConfig: SchemaConfig = BinaryInputSchemaConfig()) extends BinaryFilesReader {

  import BinaryFilesReader._

  override def read(path: String): DataFrame = {
    read(Seq(path))
  }

  override def read(paths: Seq[String]): DataFrame = {
    val rdds = paths
      .map(path => (path, md5sum(path)))
      .map(pathInfo => readToRDD(pathInfo._1, pathInfo._2)).toSeq
    val allRDDs = sparkSession.sparkContext
      .union(rdds)
    sparkSession.createDataFrame(allRDDs, schemaConfig.schema())
  }

  private def readToRDD(path: String, pathID: String): RDD[Row] = {
    sparkSession.sparkContext
      .binaryFiles(path, partitionsNum)
      .map(r => Row.fromTuple(path, pathID, r._1, r._2.toArray()))
  }


}

