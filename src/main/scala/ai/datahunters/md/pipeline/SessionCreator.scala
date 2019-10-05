package ai.datahunters.md.pipeline

import ai.datahunters.md.config.{BaseConfig, ConfigLoader, LocalModeConfig}
import org.apache.spark
import org.apache.spark.sql
import org.apache.spark.sql.SparkSession

/**
  * Creates SparkSession with appropriate configuration.
  * @param config
  * @param appName
  */
class SessionCreator(config: BaseConfig, appName: String) {

  def create(): SparkSession = {
    val builder = config match {
      case c: LocalModeConfig => createLocalSessioBuilder(c)
      case _ => throw new RuntimeException("Unsupported type of configuration.")
    }
    builder
      .appName(appName)
      .getOrCreate()
  }

  private def createLocalSessioBuilder(localConfig: LocalModeConfig): sql.SparkSession.Builder = {
    new spark.sql.SparkSession.Builder()
      .master(s"local[${localConfig.cores}]")
      .config(ConfigLoader.SparkDriverMemKey, s"${localConfig.maxMemoryGB}g")
  }

}
