package ai.datahunters.md.reader
import ai.datahunters.md.config.GeneralConfig
import ai.datahunters.md.config.reader.FilesReaderConfig
import ai.datahunters.md.schema.{BinaryInputSchemaConfig, SchemaConfig}
import com.amazonaws.regions.Region
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.slf4j.LoggerFactory

/**
  * Read binary files to DataFrame including columns described in {@link BinaryInputSchemaConfig}.
  *
  * @param sparkSession
  * @param paths All paths to directories where binary files are located.
  */
case class BasicBinaryFilesReader(sparkSession: SparkSession,
                                  config: FilesReaderConfig) extends BinaryFilesReader {

  import BinaryFilesReader._
  import BasicBinaryFilesReader.Logger

  val schemaConfig: SchemaConfig = BinaryInputSchemaConfig()
  val partitionsNum = config.partitionsNum
  val paths = config.inputPaths

  /**
    * Build DataFrame
    *
    * @return
    */
  override def load(): DataFrame = {
    config.adjustSparkConfig(sparkSession)

    val rdds = paths
      .map(readToRDD)
    val allRDDs = sparkSession.sparkContext
      .union(rdds)

    val partitions = allRDDs.partitions.length
    Logger.info(s"Number of partitions after RDDs initialization: $partitions")

    val outputDF = sparkSession.createDataFrame(allRDDs, schemaConfig.schema())
    if (partitionsNum > 0) outputDF.repartition(partitionsNum) else outputDF
  }

  private def readToRDD(path: String): RDD[Row] = {
    sparkSession.sparkContext
      .binaryFiles(path)
      .map(r => {
        val content = r._2.toArray()
        val pathHash: String = md5sum(r._1)
        Row.fromTuple(pathHash, path, r._1, content)
      })
  }


}

object BasicBinaryFilesReader {
  val Logger = LoggerFactory.getLogger(classOf[BasicBinaryFilesReader])

}
