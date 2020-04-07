package ai.datahunters.md.config

import ai.datahunters.md.UnitSpec
import com.typesafe.config.{ConfigException, ConfigFactory}

import scala.collection.JavaConversions.mapAsJavaMap

class KrbConfigSpec extends UnitSpec {

  "A KrbConfig" should "raise an error when one of keys are not set" in {
    val confInput = Map(
      s"prefix.${KrbConfig.KeytabPathKey}" -> "some/path"
    )
    val conf = ConfigFactory.parseMap(confInput)
    val c = KrbConfig.loadKrbConfig(conf, "prefix")
    assert(c === None)
    val confInput2 = Map(
      s"prefix.${KrbConfig.PrincipalKey}" -> "princ1"
    )
    val conf2 = ConfigFactory.parseMap(confInput)
    val c2 = KrbConfig.loadKrbConfig(conf2, "prefix")
    assert(c2 === None)
  }

  it should "return None if both keys are not set" in {
    val confInput = Map[String, Any]()
    val conf = ConfigFactory.parseMap(confInput)
    val c = KrbConfig.loadKrbConfig(conf, "prefix")
    assert(c === None)
  }

  it should "load both properties" in {
    val confInput = Map(
      s"prefix.${KrbConfig.KeytabPathKey}" -> "some/path/user.keytab",
      s"prefix.${KrbConfig.PrincipalKey}" -> "princ1",
      s"prefix.${KrbConfig.DebugKey}" -> true
    )
    val conf = ConfigFactory.parseMap(confInput)
    val c = KrbConfig.loadKrbConfig(conf, "prefix")
    assert(c.get.userKeytabFileName == "user.keytab")
    assert(c.get.keytabPath == "some/path/user.keytab")
    assert(c.get.principal === "princ1")
    assert(c.get.debug === true)
  }

  it should "load defaults" in {
    val confInput = Map(
      s"prefix.${KrbConfig.KeytabPathKey}" -> "some/path/user.keytab",
      s"prefix.${KrbConfig.PrincipalKey}" -> "princ1"
    )
    val conf = ConfigFactory.parseMap(confInput)
    val c = KrbConfig.loadKrbConfig(conf, "prefix")
    assert(c.get.userKeytabFileName == "user.keytab")
    assert(c.get.keytabPath == "some/path/user.keytab")
    assert(c.get.principal === "princ1")
    assert(c.get.debug === false)
  }

}

object KrbConfigSpec {

  val KeytabPath = "some/path/user.keytab"
}
