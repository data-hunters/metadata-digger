package ai.datahunters.md.processor
import ai.datahunters.md.schema.MetadataSchemaConfig.MetadataCol
import ai.datahunters.md.udf.Extractors.selectMetadataTagNames
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, explode}

/**
  * Extract directory and tag names to rows removing values to present all Metatags available in input dataset.
  */
object MetadataSummarizer extends Processor {

  val OutputFullTagNameCol = "Metadata Directory and Tag Name"
  val DirTagNamesSeparator = "."
  val IncludeDirName = true

  private val TempCol = "TmpTagNames"

  override def execute(inputDF: DataFrame): DataFrame = {
    inputDF
      .select(selectMetadataTagNames(IncludeDirName, Some(DirTagNamesSeparator))(col(MetadataCol)).as(TempCol))
      .select(explode(col(TempCol)).as(OutputFullTagNameCol))
      .distinct()
      .orderBy(col(OutputFullTagNameCol))
  }

}
