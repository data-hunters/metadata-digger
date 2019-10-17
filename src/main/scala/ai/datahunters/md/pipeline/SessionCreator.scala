package ai.datahunters.md.pipeline

import ai.datahunters.md.config.{ConfigLoader, SessionConfig}
import org.apache
import org.apache.spark
import org.apache.spark.sql
import org.apache.spark.sql.SparkSession

/**
  * Creates SparkSession with appropriate configuration.
  * @param config
  * @param appName
  */
class SessionCreator(config: SessionConfig, localMode: Boolean, appName: String) {

  def create(): SparkSession = {
    val builder = if (localMode) {
      createLocalSessioBuilder()
    } else {
      new apache.spark.sql.SparkSession.Builder
    }
    builder
      .appName(appName)
      .getOrCreate()
  }

  private def createLocalSessioBuilder(): sql.SparkSession.Builder = {
    new spark.sql.SparkSession.Builder()
      .master(s"local[${config.cores}]")
      .config(ConfigLoader.SparkDriverMemKey, s"${config.maxMemoryGB}g")
  }

}
