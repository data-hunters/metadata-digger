package ai.datahunters.md.util

import ai.datahunters.md.UnitSpec

class ParserOptSpec extends UnitSpec {

  "A Parser" should "properly parse boolean strings" in {
    assert(ParserOpt.parse[Boolean]("0").get === false)
    assert(ParserOpt.parse[Boolean]("1").get === true)
    assert(ParserOpt.parse[Boolean]("true").get === true)
    assert(ParserOpt.parse[Boolean]("false").get === false)
    assert(ParserOpt.parse[Boolean]("tRue").get === true)
    assert(ParserOpt.parse[Boolean]("faLSe").get === false)
  }

  it should "return None for wrong boolean string value" in {
    assert(ParserOpt.parse[Boolean]("invalid") === None)
    assert(ParserOpt.parse[Boolean]("") === None)
  }
}
