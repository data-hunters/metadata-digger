package ai.datahunters.md.launcher

import ai.datahunters.md.preprocessing.LocalMetadataExtractor
import ai.datahunters.md.util.TextUtils

/**
  * Extract Metadata from single file (without using Spark) from Local File System and print on the std output.
  */
object LocalBasicExtractorLauncher {


  def main(args: Array[String]): Unit = {
    if (args.length == 0) {
      println("Local path to image not provided in arguments. Closing application")
      System.exit(1)
    }
    val path = args(0)
    val metadata = LocalMetadataExtractor.extract(path)
    metadata.show()
  }
}
