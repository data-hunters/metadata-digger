package ai.datahunters.md.reader
import ai.datahunters.md.config.GeneralConfig
import ai.datahunters.md.config.reader.FilesReaderConfig
import ai.datahunters.md.schema.{BinaryInputSchemaConfig, SchemaConfig}
import com.amazonaws.regions.Region
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * Read binary files to DataFrame including columns described in {@link BinaryInputSchemaConfig}.
  *
  * @param sparkSession
  * @param partitionsNum Number of partitions. If > 0, it will make repartition. Be careful with this operation especially
  *                      in case of distributed computing.
  * @param paths All paths to directories where binary files are located.
  */
case class BasicBinaryFilesReader(sparkSession: SparkSession,
                                  config: FilesReaderConfig) extends BinaryFilesReader {

  import BinaryFilesReader._
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
    val outputDF = sparkSession.createDataFrame(allRDDs, schemaConfig.schema())
    if (partitionsNum > 0) outputDF.repartition(partitionsNum) else outputDF
  }

  private def readToRDD(path: String): RDD[Row] = {
    sparkSession.sparkContext
      .binaryFiles(path)
      .map(r => {
        val content = r._2.toArray()
        val contentHash: String = md5sum(content)
        Row.fromTuple(contentHash, path, r._1, content)
      })
  }


}

