package ai.datahunters.md.schema

import ai.datahunters.md.schema.EmbeddedMetadataSchemaConfig.MetadataCol
import org.apache.spark.sql.types.StructType

/**
  * Provide DataFrame schema for flat structure where each tag is represented by single String column.
  * Such a structure can be used for flat output formats like CSV.
  *
  * @param allowedTags
  */
case class MetadataTagsSchemaConfig (allowedTags: Seq[String]) extends SchemaConfig {
  override def columns(): Seq[String] = allowedTags

  override def schema(): StructType = new SchemaBuilder()
    .addStringFields(allowedTags)
    .build()
}

object MetadataTagsSchemaConfig {

  val MetadataCol = "Metadata"


}