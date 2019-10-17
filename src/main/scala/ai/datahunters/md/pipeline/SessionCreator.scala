package ai.datahunters.md.pipeline

import ai.datahunters.md.config.{BaseConfig, ConfigLoader, DistributedModeConfig, LocalModeConfig}
import org.apache
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
      case cl: LocalModeConfig => createLocalSessioBuilder(cl)
      case cd: DistributedModeConfig => new apache.spark.sql.SparkSession.Builder
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
