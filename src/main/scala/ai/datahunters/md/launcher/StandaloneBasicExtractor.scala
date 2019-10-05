package ai.datahunters.md.launcher

import ai.datahunters.md.config.{BaseConfig, LocalModeConfig}
import ai.datahunters.md.pipeline.{BasicExtractionWorkflow, SessionCreator}

object StandaloneBasicExtractor {

  val AppName = "Metadata-Digger"

  def main(args: Array[String]): Unit = {
    if (args.isEmpty) {
      println("Configuration path not provided in arguments. Closing application.")
      System.exit(1)
    }
    val config: BaseConfig = LocalModeConfig.buildFromProperties(args(0))

    val sessionCreator = new SessionCreator(config, AppName)
    new BasicExtractionWorkflow(config, sessionCreator).run()
  }
}
