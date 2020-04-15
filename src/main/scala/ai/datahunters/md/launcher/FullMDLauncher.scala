package ai.datahunters.md.launcher

import ai.datahunters.md.config.{ConfigLoader, Writer}
import ai.datahunters.md.config.enrich.MetadataEnrichmentConfig
import ai.datahunters.md.reader.SolrHashReader
import ai.datahunters.md.workflow.{FullMDWorkflow, Workflow}
import ai.datahunters.md.writer.{FormatAdjustmentProcessorFactory, SolrWriter}
import com.typesafe.config.Config

object FullMDLauncher {

  import BasicExtractorLauncher._

  val AppName = "Metadata-Digger [Full]"

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
    val formatAdjustmentProcessor = FormatAdjustmentProcessorFactory.create(processingConfig)
    val format = config.getString(Writer.OutputFormatKey)
    var solrHashReader: Option[SolrHashReader] = Option.empty
    if (processingConfig.processHashComparator && format.equals(SolrWriter.FormatName)) {
      solrHashReader = buildHelperSolrReader(config, sparkSession, processingConfig.namingConvention)
    }
    new FullMDWorkflow(metadataEnrichmentConfig, processingConfig, sparkSession, reader, writer, formatAdjustmentProcessor, solrHashReader = solrHashReader)
  }
}
