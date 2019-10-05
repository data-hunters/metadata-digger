package ai.datahunters.md.config

import java.io.File

import com.typesafe.config.{Config, ConfigFactory}

import scala.collection.JavaConversions.mapAsJavaMap

object ConfigLoader {

  val SparkDriverMemKey = "spark.driver.memory"

  def load(configPath: String, defaults: Map[String, _]): Config = {
    val baseConfig = ConfigFactory.parseMap(mapAsJavaMap(defaults))
    ConfigFactory.parseFile(new File(configPath)).withFallback(baseConfig)
  }


}
