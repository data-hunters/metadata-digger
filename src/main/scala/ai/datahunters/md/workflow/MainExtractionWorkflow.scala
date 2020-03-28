package ai.datahunters.md.workflow

import ai.datahunters.md.config.processing.{MandatoryTagsConfig, ProcessingConfig}
import ai.datahunters.md.filter.{Filter, NotEmptyTagFilter}
import ai.datahunters.md.pipeline.ProcessingPipeline
import ai.datahunters.md.processor._
import ai.datahunters.md.reader.{PipelineSource, SolrHashReader}
import ai.datahunters.md.writer.PipelineSink
import org.apache.spark.sql.SparkSession

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
                             analyticsFilters: Seq[Filter] = Seq(),
                             solrHashReader: Option[SolrHashReader] = None) extends Workflow {


  override def run(): Unit = {
    val mandatoryTagConfig = MandatoryTagsConfig.build(config)
    val mandatoryTagsFilter = mandatoryTagConfig.dirTags.map(d => new NotEmptyTagFilter(d))
    val columnNamesConverter = ColumnNamesConverterFactory.create(config.namingConvention)
    val rawInputDF = reader.load()
    val hashExtractor = config.hashList.map(s => HashExtractor(s))

    val hashGenerationPipeline = ProcessingPipeline(rawInputDF)
    hashExtractor.foreach(hashGenerationPipeline.addProcessor)
    var dfWithHashes = hashGenerationPipeline.run()

    if (config.processHashComparator) {
      val solrHashDF = solrHashReader.map(r => r.load())
      val hashComparator = HashComparator(solrHashDF, config.hashList.getOrElse(Seq()))
      dfWithHashes = ProcessingPipeline(dfWithHashes)
        .addProcessor(hashComparator)
        .run()
    }
    val pipeline = ProcessingPipeline(dfWithHashes)
      .setFormatAdjustmentProcessor(formatAdjustmentProcessor)
      .setColumnNamesConverter(Some(columnNamesConverter))
    if (config.thumbnailsEnabled) {
      pipeline.addProcessor(ThumbnailsGenerator(config.smallThumbnailsSize, config.mediumThumbnailsSize))
    }
    pipeline.addProcessor(MetadataExtractor())
    analyticsFilters.foreach(pipeline.addFilter)

    pipeline.addProcessor(FlattenMetadataDirectories(config.allowedDirectories))
    mandatoryTagsFilter.foreach(pipeline.addFilter)
    val extractedDF = pipeline.run()

    writer.write(extractedDF)
  }


}
