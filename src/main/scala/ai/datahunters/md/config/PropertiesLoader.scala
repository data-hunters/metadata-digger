package ai.datahunters.md.config

import java.io.FileInputStream
import java.util.Properties

import ai.datahunters.md.config.ProcessingConfig.Defaults
import com.typesafe.config.{Config, ConfigFactory}

import scala.collection.JavaConversions.mapAsJavaMap

object PropertiesLoader {

  def load(inputPath: String): Properties = {
    val props = new Properties()
    props.load(new FileInputStream(inputPath))
    props
  }



}
