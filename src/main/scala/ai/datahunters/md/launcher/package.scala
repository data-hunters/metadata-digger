package ai.datahunters.md

import ai.datahunters.md.util.ParserOpt.parse

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

    def parseComparisonArgs(args: Array[String]): ComparisonAppArguments = {
      val baseArgs = parseArgs(args)
      if (args.length != 3) {
        println("Path to base image not provided in arguments. Closing application")
        System.exit(1)
      }
      ComparisonAppArguments(baseArgs, args(2))
    }
  }

  case class BasicAppArguments(configPath: String, standaloneMode: Option[Boolean])

  case class ComparisonAppArguments(basicArgs: BasicAppArguments, baseFilePath: String)
}
