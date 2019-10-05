package ai.datahunters.md.config

import ai.datahunters.md.UnitSpec

class ConfigLoaderSpec extends UnitSpec{

  import ConfigLoaderSpec._

  "A ConfigLoader" should "load empty configuration with default values" in {
    val config = ConfigLoader.load(configPath("empty.config.properties"), Defaults)
    assert(config.getString("test.property1") === TestVal1)
    assert(config.getInt("test.property2") === TestVal2)
  }

  it should "load not empty configuration with default values" in {
    val config = ConfigLoader.load(configPath("test1.config.properties"), Defaults)
    assert(config.getString("test.property1") === "testVal123")
    assert(config.getInt("test.property2") === TestVal2)
    assert(config.getInt("test.property3") === 90)
  }
}

object ConfigLoaderSpec {

  val TestVal1 = "testVal1"
  val TestVal2 = 10

  val Defaults = Map(
    "test.property1" -> TestVal1,
    "test.property2" -> TestVal2
  )

}