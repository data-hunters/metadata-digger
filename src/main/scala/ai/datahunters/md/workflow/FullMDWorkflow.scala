package ai.datahunters.md.workflow

import ai.datahunters.md.config.enrich.MetadataEnrichmentConfig
import ai.datahunters.md.config.processing.{MandatoryTagsConfig, ProcessingConfig}
import ai.datahunters.md.filter.{Filter, NotEmptyTagFilter}
import ai.datahunters.md.pipeline.ProcessingPipeline
import ai.datahunters.md.processor._
import ai.datahunters.md.processor.enrich.MultiLabelClassifier
import ai.datahunters.md.reader.PipelineSource
import ai.datahunters.md.writer.PipelineSink
import com.intel.analytics.bigdl.utils.Engine
import com.intel.analytics.zoo.pipeline.api.Net
import org.apache.spark.sql.SparkSession

/**
  * Run full flow including:
  * * Metadata Enrichment processors
  * * Metadata Extraction processor
  * * Metadata filtering
  *
  * @param config
  * @param processingConfig
  * @param sparkSession
  * @param reader
  * @param writer
  * @param formatAdjustmentProcessor
  * @param analyticsFilters
  */
class FullMDWorkflow(config: MetadataEnrichmentConfig,
                     processingConfig: ProcessingConfig,
                     sparkSession: SparkSession,
                     reader: PipelineSource,
                     writer: PipelineSink,
                     formatAdjustmentProcessor: Option[Processor] = None,
                     analyticsFilters: Seq[Filter] = Seq()) extends Workflow {
  Engine.init
  private[workflow] val columnNamesConverter = ColumnNamesConverterFactory.create(processingConfig.namingConvention)
  private[workflow] val model = Net.load[Float](config.modelPath)
  private[workflow] val mandatoryTagConfig = MandatoryTagsConfig.build(processingConfig)
  private[workflow] val mandatoryTagsFilter = mandatoryTagConfig.dirTags.map(d => new NotEmptyTagFilter(d))

  override def run(): Unit = {
    val rawInputDF = reader.load()
    val pipeline = ProcessingPipeline(rawInputDF)
      .setFormatAdjustmentProcessor(formatAdjustmentProcessor)
      .setColumnNamesConverter(Some(columnNamesConverter))
      .addProcessor(MultiLabelClassifier(sparkSession, model, config.labelsMapping, config.threshold))
      .addProcessor(MetadataExtractor())
    analyticsFilters.foreach(pipeline.addFilter)
    pipeline.addProcessor(FlattenMetadataDirectories(processingConfig.allowedDirectories))
    mandatoryTagsFilter.foreach(pipeline.addFilter)
    val extractedDF = pipeline.run()

    writer.write(extractedDF)
  }

}
