package ai.datahunters.md.launcher

import ai.datahunters.md.config._
import ai.datahunters.md.config.reader.ReaderConfig
import ai.datahunters.md.pipeline.SessionCreator
import ai.datahunters.md.reader.PipelineSourceFactory
import ai.datahunters.md.workflow.DescribeMetadataWorkflow
import com.typesafe.config.Config

object MetadataPrinterLauncher {

  val AppName = "Metadata-Digger [Printer]"

  def main(args: Array[String]): Unit = {
    val appInputArgs = AppArguments.parseArgs(args)
    val localMode = appInputArgs.standaloneMode.getOrElse(false)
    val config: Config = ConfigLoader.load(appInputArgs.configPath)
    val sparkSession = new SessionCreator(SessionConfig.build(config), localMode, AppName).create()
    val reader = PipelineSourceFactory.create(ReaderConfig(config), sparkSession)
    DescribeMetadataWorkflow(reader).run()
  }
}
