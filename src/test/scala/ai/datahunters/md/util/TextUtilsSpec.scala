package ai.datahunters.md.util

import ai.datahunters.md.UnitSpec

class TextUtilsSpec extends UnitSpec {

  "A safeName" should "remove not safe characters" in {
    val inputText = "Some column * '(word1 ) ,.;\" "
    val expectedText = "Some column  word1   "
    val actualText = TextUtils.safeName(inputText)
    assert(actualText === expectedText)
  }

  "A camelCase" should "convert text to camel case" in {
    val inputText = "some short text"
    val expectedText = "SomeShortText"
    assert(TextUtils.camelCase(inputText) === expectedText)
  }

  "A snakeCase" should "convert text to snake case" in {
    val inputText = "Some SHort text"
    val expectedText = "some_short_text"
    assert(TextUtils.snakeCase(inputText) === expectedText)
  }


}
