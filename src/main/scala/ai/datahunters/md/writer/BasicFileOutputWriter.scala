package ai.datahunters.md.writer
import ai.datahunters.md.schema.SchemaConfig
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

case class BasicFileOutputWriter(sparkSession: SparkSession,
                                 format: String,
                                 outputFilesNum: Int) extends FileOutputWriter {

  import BasicFileOutputWriter._

  private val logger = LoggerFactory.getLogger(classOf[BasicFileOutputWriter])


  if (!AllowedFormats.contains(format)) {
    throw new RuntimeException(s"Format $format is not supported! The following file formats are supported: ${AllowedFormats.mkString(",")}")
  }

  override def write(data: DataFrame, path: String): Unit = {
    val outputDF = if (currentPartitionsNum(data) != outputFilesNum) {
      logger.warn("Changing number of partitions to achieve specific number of output files.")
      data.repartition(outputFilesNum)
    } else data
    outputDF.write
      .format(format)
      .option("header", true)
      .save(path)
  }

  private def currentPartitionsNum(data: DataFrame): Int = data.rdd
    .partitions
    .length
}

object BasicFileOutputWriter {

  val AllowedFormats = Seq(
    FileOutputWriter.CsvFormat,
    FileOutputWriter.JsonFormat
  )
}