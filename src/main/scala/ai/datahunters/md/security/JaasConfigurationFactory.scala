package ai.datahunters.md.security

import com.sun.security.auth.module.Krb5LoginModule
import javax.security.auth.login.{AppConfigurationEntry, Configuration}

/**
  * Build configuration for JaaS setting properties programmatically instead of passing path to jaas.conf file.
  * It was necessary to pass path to keytab internally using path to Spark workspace directory.
  */
object JaasConfigurationFactory {

  import scala.collection.JavaConversions._

  private val PrincipalKey = "principal"
  private val KeytabKey = "keyTab"
  private val DebugKey = "debug"
  private val DefaultKeytabConf = Map(
    "doNotPrompt" -> "true",
    "storeKey" -> "true",
    "useKeyTab" -> "true",
    "useTicketCache" -> "false"
  )

  def create(keytabPath: String, principal: String, debug: Boolean): Configuration = {
    val conf = DefaultKeytabConf ++ Map(
      PrincipalKey -> principal,
      KeytabKey -> keytabPath,
      DebugKey -> debug.toString
    )
    val entries = createEntries(conf)
    new Configuration {
      override def getAppConfigurationEntry(name: String): Array[AppConfigurationEntry] = entries
    }
  }

  private def createEntries(conf: Map[String, _]): Array[AppConfigurationEntry] = {
    Array[AppConfigurationEntry](
      new AppConfigurationEntry(
        classOf[Krb5LoginModule].getCanonicalName(),
        AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
        conf)
    )
  }
}
