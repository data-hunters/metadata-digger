package ai.datahunters.md

import ai.datahunters.md.util.Parser.parse

package object launcher {

  object AppArguments {

    def parseArgs(args: Array[String]): BasicAppArguments = {
      if (args.isEmpty) {
        println("Configuration path not provided in arguments. Closing application.")
        System.exit(1)
      }
      val configPath = args(0)
      val mode = if (args.length > 1) parse[Boolean](args(1)) else None
      BasicAppArguments(configPath, mode)
    }
  }

  case class BasicAppArguments(configPath: String, standaloneMode: Option[Boolean])
}
