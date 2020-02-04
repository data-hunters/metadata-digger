package ai.datahunters.md.processor
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
  * Convert array columns to strings concatenating with specified delimiter.
  *
  * @param delimiter
  * @param columns
  */
case class FlattenArrays(delimiter: String, columns: Seq[String]) extends Processor {

  override def execute(inputDF: DataFrame): DataFrame = {
    columns.foldLeft(inputDF)((previousDF, colName) => previousDF.withColumn(colName, concat_ws(delimiter, col(colName))))
  }

}
