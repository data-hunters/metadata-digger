package ai.datahunters.md.writer

import org.apache.spark.sql.DataFrame

trait FileOutputWriter extends PipelineSink {

  def write(data: DataFrame): Unit

}

object FileOutputWriter {

  val CsvFormat = "csv"
  val JsonFormat = "json"
}