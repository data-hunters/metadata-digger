package ai.datahunters.md.launcher

import ai.datahunters.md.config._
import ai.datahunters.md.config.processing.ProcessingConfig
import ai.datahunters.md.config.reader.ReaderConfig
import ai.datahunters.md.config.writer.WriterConfig
import ai.datahunters.md.pipeline.SessionCreator
import ai.datahunters.md.reader.PipelineSourceFactory
import ai.datahunters.md.util.Parser.parse
import ai.datahunters.md.workflow.BasicExtractionWorkflow
import ai.datahunters.md.writer.{FormatAdjustmentProcessorFactory, PipelineSinkFactory}
import com.typesafe.config.Config

object BasicExtractorLauncher {

  val AppName = "Metadata-Digger [Basic Extraction]"

  def main(args: Array[String]): Unit = {
    val appInputArgs = AppArguments.parseArgs(args)
    val localMode = appInputArgs.standaloneMode.getOrElse(false)
    val config: Config = ConfigLoader.load(appInputArgs.configPath)
    val sessionCreator = new SessionCreator(SessionConfig.build(config), localMode, AppName)
    val sparkSession = sessionCreator.create()
    val reader = PipelineSourceFactory.create(ReaderConfig(config), sparkSession)
    val writer = PipelineSinkFactory.create(WriterConfig(config), sparkSession)
    val format = config.getString(Writer.OutputFormatKey)
    val processingConfig = ProcessingConfig.build(config)
    val formatAdjustmentProcessor = FormatAdjustmentProcessorFactory.create(processingConfig)
    new BasicExtractionWorkflow(processingConfig, sparkSession, reader, writer, formatAdjustmentProcessor).run()
  }

}
