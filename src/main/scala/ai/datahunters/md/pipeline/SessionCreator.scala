package ai.datahunters.md.pipeline

import ai.datahunters.md.config.{ConfigLoader, SessionConfig}
import com.intel.analytics.zoo.common.NNContext
import org.apache
import org.apache.spark
import org.apache.spark.{SparkConf, sql}
import org.apache.spark.sql.SparkSession

/**
  * Creates SparkSession with appropriate configuration.
  * @param config
  * @param appName
  */
class SessionCreator(config: SessionConfig, localMode: Boolean, appName: String) {

  def create(sparkConf: SparkConf = NNContext.createSparkConf()): SparkSession = {
    val builder = if (localMode) {
      createLocalSessionBuilder(sparkConf)
    } else {
      new apache.spark.sql.SparkSession.Builder()
        .config(sparkConf)

    }
    builder
      .appName(appName)
      .getOrCreate()
  }

  private def createLocalSessionBuilder(sparkConf: SparkConf): sql.SparkSession.Builder = {

    new spark.sql.SparkSession.Builder()
      .master(s"local[${config.cores}]")
      .config(sparkConf)
      .config(ConfigLoader.SparkDriverMemKey, s"${config.maxMemoryGB}g")
  }

}
