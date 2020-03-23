package ai.datahunters.md.config

import java.nio.file.Paths

import com.typesafe.config.Config
import org.slf4j.LoggerFactory

case class KrbConfig(principal: String, keytabPath: String, debug: Boolean) {

  val userKeytabFileName: String = Paths.get(keytabPath)
    .getFileName
    .toString
}

object KrbConfig {

  val Logger = LoggerFactory.getLogger(classOf[KrbConfig])

  val SecurityPrefix = "security"

  val KeytabPathKey = s"$SecurityPrefix.keytabPath"
  val PrincipalKey = s"$SecurityPrefix.principal"
  val DebugKey = s"$SecurityPrefix.debug"

  def buildDefaults(prefix: String) = Map(
    keytabPathKey(prefix) -> "",
    principalKey(prefix) -> "",
    debugKey(prefix) -> false
  )

  private def keytabPathKey(prefix: String) = s"$prefix.$KeytabPathKey"

  private def principalKey(prefix: String) = s"$prefix.$PrincipalKey"

  private def debugKey(prefix: String) = s"$prefix.$DebugKey"

  def loadKrbConfig(config: Config, prefix: String): Option[KrbConfig] = {
    val configWithDefatuls = ConfigLoader.assignDefaults(config, buildDefaults(prefix))
    val keytabPath = configWithDefatuls.getString(keytabPathKey(prefix))
    val principal = configWithDefatuls.getString(principalKey(prefix))

    if (keytabPath.isEmpty || principal.isEmpty) {
      None
    } else {
      val debug = configWithDefatuls.getBoolean(debugKey(prefix))
      Logger.info("Kerberos properties have been set:")
      Logger.info(s"${keytabPathKey(prefix)}=$keytabPath")
      Logger.info(s"${principalKey(prefix)}=$principal")
      Logger.info(s"${debugKey(prefix)}=$debug")
      Some(KrbConfig(principal, keytabPath, debug))
    }
  }

  private def loadProps(key: String, config: Config): String = {
    if (config.hasPath(key)) {
      val kp = config.getString(key)
      Logger.info(s"Kerberos property ($key): $kp")
      kp
    } else {
      Logger.info(s"Kerberos property $key not set.")
      ""
    }
  }
}