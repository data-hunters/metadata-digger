package ai.datahunters.md.util

import ai.datahunters.md.UnitSpec

class ParserSpec extends UnitSpec {

  "A Parser" should "parse integer value" in {
    assert(Parser.parse[Int]("123aa.23dd") === 123)
    assert(Parser.parse[Int]("123.9") === 123)
    assert(Parser.parse[Int]("-562d") === -562)
  }

  it should "throw exception in case of invalid integer value" in {
    intercept[NumberFormatException] {
      Parser.parse[Int]("")
    }
  }

  it should "parse float value" in {
    assert((Parser.parse[Float]("123.23dd") * 100).toInt === 12323)
    assert((Parser.parse[Float]("123.9") * 10).toInt === 1239)
    assert((Parser.parse[Float]("-562.2") * 10).toInt === -5622)
  }

  it should "throw exception in case of invalid float value" in {
    intercept[NumberFormatException] {
      Parser.parse[Float]("")
    }
  }

  it should "parse long value" in {
    assert(Parser.parse[Long](s"${Long.MaxValue}asd") === Long.MaxValue)
    assert(Parser.parse[Long](s"${Long.MinValue} pixels") === Long.MinValue)
    assert(Parser.parse[Long]("-562d") === -562L)
  }

  it should "throw exception in case of invalid long value" in {
    intercept[NumberFormatException] {
      Parser.parse[Long]("")
    }
  }
}
