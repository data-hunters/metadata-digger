package ai.datahunters.md.writer
import ai.datahunters.md.config.ConfigLoader
import ai.datahunters.md.config.writer.FilesWriterConfig
import ai.datahunters.md.schema.SchemaConfig
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

case class BasicFileOutputWriter(sparkSession: SparkSession,
                                 config: FilesWriterConfig) extends FileOutputWriter {

  import BasicFileOutputWriter._

  private val logger = LoggerFactory.getLogger(classOf[BasicFileOutputWriter])


  if (!AllowedFormats.contains(config.format)) {
    throw new RuntimeException(s"Format ${config.format} is not supported! The following file formats are supported: ${AllowedFormats.mkString(",")}")
  }

  override def write(data: DataFrame): Unit = {
    config.adjustSparkConfig(sparkSession)
    val outputDF = if (config.outputFilesNum > 0 && currentPartitionsNum(data) != config.outputFilesNum) {
      logger.warn("Changing number of partitions to achieve specific number of output files.")
      data.repartition(config.outputFilesNum)
    } else data
    val df = outputDF.write
      .format(config.format)
      .options(Options.get(config.format).getOrElse(Map()))
      .save(config.outputDirPath)
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

  val Options = Map(
    FileOutputWriter.CsvFormat -> Map(
      "header" -> "true",
      "quoteAll" -> "true"
    )
  )

}