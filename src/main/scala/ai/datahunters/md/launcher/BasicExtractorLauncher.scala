package ai.datahunters.md.launcher

import ai.datahunters.md.config._
import ai.datahunters.md.config.reader.ReaderConfig
import ai.datahunters.md.config.writer.WriterConfig
import ai.datahunters.md.pipeline.{BasicExtractionWorkflow, SessionCreator}
import ai.datahunters.md.reader.PipelineSourceFactory
import ai.datahunters.md.util.Parser.parse
import ai.datahunters.md.writer.{FormatAdjustmentProcessorFactory, PipelineSinkFactory}
import com.typesafe.config.Config

object BasicExtractorLauncher {

  val AppName = "Metadata-Digger"

  def main(args: Array[String]): Unit = {
    if (args.isEmpty) {
      println("Configuration path not provided in arguments. Closing application.")
      System.exit(1)
    }
    val mode = if (args.length > 1) parse[Boolean](args(1)) else None
    val localMode = mode.getOrElse(false)
    val config: Config = ConfigLoader.load(args(0))
    val sessionCreator = new SessionCreator(SessionConfig.build(config), localMode, AppName)
    val sparkSession = sessionCreator.create()
    val reader = PipelineSourceFactory.create(ReaderConfig.build(config), sparkSession)
    val writer = PipelineSinkFactory.create(WriterConfig.build(config), sparkSession)
    val format = config.getString(Writer.OutputFormatKey)
    val processingConfig = ProcessingConfig.build(config)
    val formatAdjustmentProcessor = FormatAdjustmentProcessorFactory.create(format, processingConfig.includeDirsInTags, processingConfig.metadataColumnsPrefix)
    new BasicExtractionWorkflow(processingConfig, sparkSession, reader, writer, formatAdjustmentProcessor).run()
  }

}
