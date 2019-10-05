package ai.datahunters.md.schema

import ai.datahunters.md.UnitSpec

//TODO: Test types of fields
class MetadataSchemaConfigSpec extends UnitSpec {

  "A MetadataSchemaConfig" should "keep info about schema for Metadata with Directories as fields" in {
    val fields = Seq("col1", "col3", "col2")
    assert(MetadataSchemaConfig(fields).columns() === fields)
  }
}
