package ai.datahunters.md.processor
import org.apache.spark.sql.DataFrame

case class DropColumns(columns: Seq[String]) extends Processor {
  override def execute(inputDF: DataFrame): DataFrame = {
    inputDF.drop(columns:_*)
  }
}
