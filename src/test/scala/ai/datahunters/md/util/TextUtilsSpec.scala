package ai.datahunters.md.util

import ai.datahunters.md.UnitSpec

class TextUtilsSpec extends UnitSpec {

  "A TextUtils" should "remove not safe characters" in {
    val inputText = "Some column * '(word1 ) ,.;\" "
    val expectedText = "Somecolumnword1"
    assert(expectedText === TextUtils.safeName(inputText))
  }

  it should "convert text to camel case" in {
    val inputText = "some short text"
    val expectedText = "SomeShortText"
    assert(expectedText === TextUtils.camelCase(inputText))
  }

}
