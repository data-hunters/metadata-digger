package ai.datahunters.md.filter
import ai.datahunters.md.schema.MetadataSchemaConfig
import org.apache.spark.sql.DataFrame

class NotEmptyMetadataDir(dirName: String) extends Filter {
  import ai.datahunters.md.udf.Filters._
  import org.apache.spark.sql.functions._

  override def execute(inputDF: DataFrame): DataFrame = {
    inputDF.filter(notEmptyMapUDF(col(buildColName())))
  }

  private def buildColName(): String = s"${MetadataSchemaConfig.MetadataCol}.$dirName"
}
