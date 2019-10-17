package ai.datahunters.md.config

import ai.datahunters.md.config.ConfigLoader.assignDefaults
import com.typesafe.config.Config

case class SessionConfig(val cores: Int,
                         val maxMemoryGB: Int) {

}

object SessionConfig {

  val CoresKey = "processing.cores"
  val MaxMemoryGBKey = "processing.maxMemoryGB"

  val Defaults = Map(
    CoresKey -> defaultCores(),
    MaxMemoryGBKey -> 2
  )

  def build(config: Config): SessionConfig = {
    val configWithDefaults = assignDefaults(config, Defaults)
    SessionConfig(
      configWithDefaults.getInt(CoresKey),
      configWithDefaults.getInt(MaxMemoryGBKey)
    )
  }

  private def defaultCores(): Int = Math.max(1, Runtime.getRuntime.availableProcessors - 1)
}