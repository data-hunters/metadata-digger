package ai.datahunters.md.schema
import ai.datahunters.md.schema.MetadataTagsSchemaConfig.MetadataCol
import org.apache.spark.sql.types.StructType

/**
  * Provide DataFrame schema for embedded structure of metadata where each group/directory is representing by key and contains all related tags as map.
  * Final structure can be read as Map[String, Map[String, String] ]
  */
case class EmbeddedMetadataSchemaConfig() extends SchemaConfig {

  import EmbeddedMetadataSchemaConfig._

  override def columns(): Seq[String] = schema().fields
    .map(_.name)
    .toSeq

  override def schema(): StructType = new SchemaBuilder()
      .addEmbeddedMap(TagsCol)
      .addStringArrayField(DirectoryNames)
      .addStringArrayField(TagNamesCol)
      .addIntField(TagsCountCol)
      .addStringField(FileTypeCol)
      .build()

}

object EmbeddedMetadataSchemaConfig {

  /**
    * Name of root column which keeps embedded structure of all fields related to metadata.
    */
  val MetadataCol = "Metadata"

  /**
    * Name of column keeping all tags in embedded structure.
    */
  val TagsCol = "Tags"

  /**
    * Name of column keeping list of all existing (for this row) directory/group names of tags.
    */
  val DirectoryNames = "Directory Names"

  /**
    * Name of column keeping list of all existing (for this row) tag names.
    */
  val TagNamesCol = "Tag Names"

  /**
    * Number of all tags for this row
    */
  val TagsCountCol = "Tags Count"

  /**
    * Type of file
    */
  val FileTypeCol = "File Type"

  val FullTagsCol = s"${MetadataCol}.${TagsCol}"
  val FullDirectoriesCol = s"${MetadataCol}.${DirectoryNames}"
  val FullTagNamesCol = s"${MetadataCol}.${TagNamesCol}"
  val FullTagsCountCol = s"${MetadataCol}.${TagsCountCol}"
  val FullFileTypeCol = s"${MetadataCol}.${FileTypeCol}"
}
