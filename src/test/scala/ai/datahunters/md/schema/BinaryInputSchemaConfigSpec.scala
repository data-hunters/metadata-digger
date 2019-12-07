package ai.datahunters.md.schema

import ai.datahunters.md.UnitSpec

//TODO: Test types of fields
class BinaryInputSchemaConfigSpec extends UnitSpec {
  import BinaryInputSchemaConfig._

  "An BinaryInputSchemaConfigSpec" should "keep info about input schema for binary files" in {
    val schemaConfig = BinaryInputSchemaConfig()
    assert(schemaConfig.columns() === Seq(ID, BasePathCol, FilePathCol, FileCol))
  }

}
