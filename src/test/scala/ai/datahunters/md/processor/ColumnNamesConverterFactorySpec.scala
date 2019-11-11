package ai.datahunters.md.processor

import ai.datahunters.md.UnitSpec

class ColumnNamesConverterFactorySpec extends UnitSpec {

  "A ColumnNamesconverterFactory" should "create camel case converter" in {
    val outputText = ColumnNamesConverterFactory.create("camelCase")
      .namingConvention("This is some SenTence")
    assert(outputText === "ThisIsSomeSenTence")
  }

  it should "create snake case converter" in {
    val outputText = ColumnNamesConverterFactory.create("snakeCase")
      .namingConvention("This is some SenTence")
    assert(outputText === "this_is_some_sentence")
  }

  it should "throw exceptin due to lack of invalidConverter" in {
    intercept[RuntimeException] {
      ColumnNamesConverterFactory.create("invalidConverter")
        .namingConvention("This is some SenTence")
    }
  }
}
