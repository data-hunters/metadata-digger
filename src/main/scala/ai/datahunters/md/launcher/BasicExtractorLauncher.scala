package ai.datahunters.md.launcher

import ai.datahunters.md.config._
import ai.datahunters.md.config.processing.ProcessingConfig
import ai.datahunters.md.config.reader.ReaderConfig
import ai.datahunters.md.config.writer.{SolrWriterConfig, WriterConfig}
import ai.datahunters.md.filter.Filter
import ai.datahunters.md.pipeline.SessionCreator
import ai.datahunters.md.reader.{PipelineSource, PipelineSourceFactory, SolrHashReader}
import ai.datahunters.md.workflow.{MainExtractionWorkflow, Workflow}
import ai.datahunters.md.writer.{FormatAdjustmentProcessorFactory, PipelineSink, PipelineSinkFactory, SolrWriter}
import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession

object BasicExtractorLauncher {

  val AppName = "Metadata-Digger [Basic Extraction]"

  def main(args: Array[String]): Unit = {
    val appInputArgs = AppArguments.parseArgs(args)
    val config: Config = ConfigLoader.load(appInputArgs.configPath)
    buildWorkflow(appInputArgs, config).run()
  }

  private[launcher] def loadProcessingConfig(config: Config): ProcessingConfig = ProcessingConfig.build(config)

  private[launcher] def loadSession(appName: String, config: Config, localMode: Boolean): SparkSession = {
    val sessionCreator = new SessionCreator(SessionConfig.build(config), localMode, appName)
    sessionCreator.create()
  }

  private[launcher] def buildReader(config: Config, sparkSession: SparkSession): PipelineSource = PipelineSourceFactory.create(ReaderConfig(config), sparkSession)

  private[launcher] def buildWriter(config: Config, sparkSession: SparkSession): PipelineSink = PipelineSinkFactory.create(WriterConfig(config), sparkSession)

  private[launcher] def buildAlternativeReader(config: Config, sparkSession: SparkSession) = Option(SolrHashReader(sparkSession, SolrWriterConfig.build(config)))

  private[launcher] def buildWorkflow(appInputArgs: BasicAppArguments, config: Config, analyticsFilters: Seq[Filter] = Seq()): Workflow = {
    val localMode = appInputArgs.standaloneMode.getOrElse(true)
    val sparkSession = loadSession(AppName, config, localMode)
    val reader = buildReader(config, sparkSession)
    val writer = buildWriter(config, sparkSession)
    val format = config.getString(Writer.OutputFormatKey)
    val processingConfig = loadProcessingConfig(config)
    val formatAdjustmentProcessor = FormatAdjustmentProcessorFactory.create(processingConfig)
    var solrHashReader: Option[SolrHashReader] = Option.empty
    if (processingConfig.processHashComparator && format.equals(SolrWriter.FormatName)) {
      solrHashReader = buildAlternativeReader(config, sparkSession)
    }
    new MainExtractionWorkflow(processingConfig, sparkSession, reader, writer, formatAdjustmentProcessor, analyticsFilters, solrHashReader)
  }

}
