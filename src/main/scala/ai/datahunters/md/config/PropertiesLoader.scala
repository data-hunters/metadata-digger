package ai.datahunters.md.config

import java.io.FileInputStream
import java.util.Properties

object PropertiesLoader {

  def load(inputPath: String): Properties = {
    val props = new Properties()
    props.load(new FileInputStream(inputPath))
    props
  }

}
