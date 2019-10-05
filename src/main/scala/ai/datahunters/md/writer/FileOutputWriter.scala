package ai.datahunters.md.writer

import org.apache.spark.sql.DataFrame

trait FileOutputWriter {

  def write(data: DataFrame, path: String): Unit

}

object FileOutputWriter {

  val CsvFormat = "csv"
  val JsonFormat = "json"
}