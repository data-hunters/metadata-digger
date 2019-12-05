package ai.datahunters.md.workflow

import ai.datahunters.md.config.processing.{MandatoryTagsConfig, ProcessingConfig}
import ai.datahunters.md.filter.{Filter, NotEmptyTagFilter}
import ai.datahunters.md.pipeline.ProcessingPipeline
import ai.datahunters.md.processor.{ColumnNamesConverterFactory, FlattenMetadataDirectories, MetadataExtractor, Processor}
import ai.datahunters.md.reader.PipelineSource
import ai.datahunters.md.writer.PipelineSink
import org.apache.spark.sql.SparkSession
import org.apache.zookeeper.KeeperException.NotEmptyException

/**
  * Main workflow which run the following steps:
  * * Loading binary files from multiple paths
  * * Extracting metadata to embedded column (Map[String, Map[String, String]])
  * * Optional filtering analytics
  * * Flattening metadata to achieve remove one level of embedding
  * * Format adjustment transformations
  * * Saving output to file(s)
  *
  * @param config
  * @param sessionCreator
  */
class MainExtractionWorkflow(config: ProcessingConfig,
                             sparkSession: SparkSession,
                             reader: PipelineSource,
                             writer: PipelineSink,
                             formatAdjustmentProcessor: Option[Processor] = None,
                             analyticsFilters: Seq[Filter] = Seq()) extends Workflow {


  override def run(): Unit = {
    val mandatoryTagConfig = MandatoryTagsConfig.build(config)
    var mandatoryTagsFilter: Option[NotEmptyTagFilter] = None
    if (mandatoryTagConfig.dirTags.isDefined) mandatoryTagsFilter =
      Option(new NotEmptyTagFilter(mandatoryTagConfig.dirTags.get))
    val columnNamesConverter = ColumnNamesConverterFactory.create(config.namingConvention)
    val rawInputDF = reader.load()
    val pipeline = ProcessingPipeline(rawInputDF)
      .setFormatAdjustmentProcessor(formatAdjustmentProcessor)
      .setColumnNamesConverter(Some(columnNamesConverter))
      .addProcessor(MetadataExtractor())
    analyticsFilters.foreach(pipeline.addFilter)
    pipeline.addProcessor(FlattenMetadataDirectories(config.allowedDirectories))
    mandatoryTagsFilter.foreach(pipeline.addFilter)
    val extractedDF = pipeline.run()

    writer.write(extractedDF)
  }


}
