package ai.datahunters.md.schema

import ai.datahunters.md.schema.EmbeddedMetadataSchemaConfig.MetadataCol
import org.apache.spark.sql.types.StructType

/**
  * Provide DataFrame schema for flat structure where each tag is represented by single String column.
  * Such a structure can be used for flat output formats like CSV.
  *
  * @param allowedTags
  */
case class MetadataTagsSchemaConfig (colPrefix: String, allowedTags: Seq[String]) extends SchemaConfig {

  protected val finalAllowedTags = allowedTags.map(t => s"$colPrefix$t")
  override def columns(): Seq[String] = finalAllowedTags

  override def schema(): StructType = new SchemaBuilder()
    .addStringFields(finalAllowedTags)
    .build()
}

object MetadataTagsSchemaConfig {

  val MetadataCol = "Metadata"

  val TagsCol = "Tags"
  val DirectoriesCol = "Directories"


}