package ai.datahunters.md.udf

import ai.datahunters.md.UnitSpec
import ai.datahunters.md.udf.Converters.Transformations

class ConvertersSpec extends UnitSpec {

  "convertMapKeysT" should "convert keys by removing spaces" in {
    val testInput = Map("test col 1" -> "val1", "test col 2" -> "val2")
    val output = Transformations.convertMapKeysT((s: String) => s.replace(" ", ""))(testInput)
    assert(output.keySet === Seq("testcol1", "testcol2").toSet)
  }

}
