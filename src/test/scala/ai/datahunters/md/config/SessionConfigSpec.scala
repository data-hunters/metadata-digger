package ai.datahunters.md.config

import ai.datahunters.md.UnitSpec
import com.typesafe.config.ConfigFactory
import scala.collection.JavaConversions.mapAsJavaMap

class SessionConfigSpec extends UnitSpec {

  "A SessionConfig" should "load default values" in {
    val inputConfig = Map[String, AnyVal]()
    val config = ConfigFactory.parseMap(inputConfig)
    val outputConfig = SessionConfig.build(config)
    assert(outputConfig.cores === Math.max(1, Runtime.getRuntime.availableProcessors - 1))
    assert(outputConfig.maxMemoryGB === 2)
  }
}
