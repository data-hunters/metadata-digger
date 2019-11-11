package ai.datahunters.md.processor
import ai.datahunters.md.schema._
import ai.datahunters.md.udf.Extractors
import org.apache.spark.sql.DataFrame

case class FlattenMetadataDirectories(allowedDirectories: Option[Seq[String]] = None) extends Processor {
  import EmbeddedMetadataSchemaConfig._
  import org.apache.spark.sql.functions._
  import FlattenMetadataDirectories._
  import Extractors.selectMetadata

  override def execute(inputDF: DataFrame): DataFrame = {
    val selectedDirs = retrieveDirectories(inputDF)
    val selectMetadataUDF = selectMetadata(selectedDirs)
    inputDF.withColumn(EmbeddedMetadataSchemaConfig.DirectoriesCol, col(EmbeddedMetadataSchemaConfig.FullDirectoriesCol))
      .withColumn(MetadataSchemaConfig.MetadataCol, selectMetadataUDF(
        col(EmbeddedMetadataSchemaConfig.FullTagsCol)
      ))
  }

  private def retrieveDirectories(inputDF: DataFrame): Seq[String] = {
    allowedDirectories.getOrElse({
      inputDF.cache()
      val availableDirectories = inputDF.select(explode(col(FullDirectoriesCol)).as(TempDirCol))
        .distinct()
        .collect()
        .map(_.getAs[String](TempDirCol))
      availableDirectories.toSeq
    })
  }
}


object FlattenMetadataDirectories {

  private val TempDirCol = "Dir"
}
