package ai.datahunters.md.schema

import ai.datahunters.md.UnitSpec

//TODO: Test types of fields
class MetadataTagsSchemaConfigSpec extends UnitSpec {

  "A MetadataTagsSchemaConfig" should "keep info about metadata schema where with tags as columns" in {
    val tags = Seq("tag1", "tag4", "tag2")
    assert(MetadataTagsSchemaConfig(tags).columns() === tags)
  }

}
