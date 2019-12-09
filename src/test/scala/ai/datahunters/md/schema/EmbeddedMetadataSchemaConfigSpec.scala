package ai.datahunters.md.schema

import ai.datahunters.md.UnitSpec

//TODO: Test types of fields
class EmbeddedMetadataSchemaConfigSpec extends UnitSpec {
  import EmbeddedMetadataSchemaConfig._

  "An EmbeddedMetadataSchemaConfig" should "keep info about metadata schema where directories and tags are embedded in map" in {
    val cols = EmbeddedMetadataSchemaConfig().columns()
    val expectedCols = Seq(TagsCol, DirectoryNamesCol, TagNamesCol, TagsCountCol)
  }
}
