package ai.datahunters.md.schema
import org.apache.spark.sql.types.StructType

/**
  * Provides DataFrame schema for metadata where each group/directory of tags contains key/value list (map) of particular tags.
  *
  * @param allowedDirs
  */
case class MetadataSchemaConfig(allowedDirs: Seq[String]) extends SchemaConfig {
  override def columns(): Seq[String] = allowedDirs

  override def schema(): StructType = new SchemaBuilder()
    .addStringMaps(allowedDirs)
    .build()
}

object MetadataSchemaConfig {

  /**
    * Name of column containing all columns representing groups/directories.
    */
  val MetadataCol = "Metadata"

}
