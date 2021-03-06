package ai.datahunters.md.launcher

import ai.datahunters.md.config.ConfigLoader
import ai.datahunters.md.config.enrich.MetadataEnrichmentConfig
import ai.datahunters.md.workflow.{MetadataEnrichmentWorkflow, Workflow}
import ai.datahunters.md.writer.FormatAdjustmentProcessorFactory
import com.typesafe.config.Config

object MetadataEnrichmentLauncher {
  import BasicExtractorLauncher._

  val AppName = "Metadata-Digger [Enrichment]"

  def main(args: Array[String]): Unit = {
    val appInputArgs = AppArguments.parseArgs(args)
    val config: Config = ConfigLoader.load(appInputArgs.configPath)
    buildWorkflow(appInputArgs, config).run()
  }

  private[launcher] def buildWorkflow(appInputArgs: BasicAppArguments, config: Config): Workflow = {
    val localMode = appInputArgs.standaloneMode.getOrElse(true)
    val sparkSession = loadSession(AppName, config, localMode)
    val reader = buildReader(config, sparkSession)
    val writer = buildWriter(config, sparkSession)
    val processingConfig = loadProcessingConfig(config)
    val metadataEnrichmentConfig = MetadataEnrichmentConfig.build(config)
    metadataEnrichmentConfig.adjustSparkConfig(sparkSession)
    val formatAdjustmentProcessor = FormatAdjustmentProcessorFactory.create(metadataEnrichmentConfig, processingConfig.outputFormat)

    new MetadataEnrichmentWorkflow(metadataEnrichmentConfig, processingConfig, sparkSession, reader, writer, formatAdjustmentProcessor)
  }
}
