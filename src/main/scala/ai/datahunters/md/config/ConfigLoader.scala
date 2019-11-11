package ai.datahunters.md.config

import java.io.File

import com.typesafe.config.{Config, ConfigFactory}

import scala.collection.JavaConversions.mapAsJavaMap

object ConfigLoader {

  val SparkDriverMemKey = "spark.driver.memory"
  val ListElementsDelimiter = ","
  val All = "*"



  implicit def strToSeq(configVal: String): Option[Seq[String]] = {
    if (configVal.equalsIgnoreCase(All)) {
      None
    } else {
      Some(
        configVal.split(ListElementsDelimiter).map(_.trim).toSeq
      )
    }
  }



  def load(configPath: String, defaults: Map[String, _] = Map()): Config = {
    val baseConfig = ConfigFactory.parseMap(mapAsJavaMap(defaults))
    ConfigFactory.parseFile(new File(configPath)).withFallback(baseConfig)
  }

  def assignDefaults(config: Config, defaults: Map[String, _]): Config = {
    val defaultsConfig = ConfigFactory.parseMap(mapAsJavaMap(defaults))
    config.withFallback(defaultsConfig)
  }

}
