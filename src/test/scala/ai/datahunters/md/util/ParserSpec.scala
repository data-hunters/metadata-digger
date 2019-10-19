package ai.datahunters.md.util

import ai.datahunters.md.UnitSpec

class ParserSpec extends UnitSpec {

  "A Parser" should "properly parse boolean strings" in {
    assert(Parser.parse[Boolean]("0").get === false)
    assert(Parser.parse[Boolean]("1").get === true)
    assert(Parser.parse[Boolean]("true").get === true)
    assert(Parser.parse[Boolean]("false").get === false)
    assert(Parser.parse[Boolean]("tRue").get === true)
    assert(Parser.parse[Boolean]("faLSe").get === false)
  }

  it should "return None for wrong boolean string value" in {
    assert(Parser.parse[Boolean]("invalid") === None)
    assert(Parser.parse[Boolean]("") === None)
  }
}
