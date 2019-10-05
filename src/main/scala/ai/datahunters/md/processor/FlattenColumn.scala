package ai.datahunters.md.processor
import ai.datahunters.md.schema.{MetadataSchemaConfig, SchemaConfig}
import org.apache.spark.sql.DataFrame

case class FlattenColumn(column: String) extends Processor {

  override def execute(inputDF: DataFrame): DataFrame = {
    val columns = SchemaConfig.dfExistingColumns(inputDF, Seq(column))
    val finalColumns: Seq[String] = columns ++ Seq(s"${column}.*")
    inputDF.select(finalColumns.head, finalColumns.tail:_*)
  }

}
