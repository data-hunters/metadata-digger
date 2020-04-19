package ai.datahunters.md.workflow

import ai.datahunters.md.config.enrich.MetadataEnrichmentConfig
import ai.datahunters.md.config.processing.{MandatoryTagsConfig, ProcessingConfig}
import ai.datahunters.md.filter.{Filter, NotEmptyTagFilter}
import ai.datahunters.md.pipeline.ProcessingPipeline
import ai.datahunters.md.processor.HashExtractor.HashColPrefix
import ai.datahunters.md.processor._
import ai.datahunters.md.processor.enrich.MultiLabelClassifier
import ai.datahunters.md.reader.{PipelineSource, SolrHashReader}
import ai.datahunters.md.util.FilesHandler
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
                     analyticsFilters: Seq[Filter] = Seq(),
                     solrHashReader: Option[SolrHashReader] = None) extends Workflow {
  Engine.init
  private[workflow] val columnNamesConverter = ColumnNamesConverterFactory.create(processingConfig.namingConvention)

  private[workflow] def loadModel() = {
    val finalPath = FilesHandler.sparkFilePath(config.modelFileName)
    Net.load[Float](finalPath)
  }

  private[workflow] val mandatoryTagConfig = MandatoryTagsConfig.build(processingConfig)
  private[workflow] val mandatoryTagsFilter = mandatoryTagConfig.dirTags.map(d => new NotEmptyTagFilter(d))

  override def run(): Unit = {
    val rawInputDF = reader.load()
    val hashList = processingConfig.hashList
      .map(s => s.filter(e => e.nonEmpty))
    val hashColumns: Option[Seq[String]] = hashList.map(s=>s.map(s => HashColPrefix + s)
      .map(s => columnNamesConverter.namingConvention(s)))
    val hashExtractor = hashList.map(s => HashExtractor(s, columnNamesConverter))
    val hashGenerationPipeline = ProcessingPipeline(rawInputDF)
    hashExtractor.foreach(hashGenerationPipeline.addProcessor)
    var dfWithHashes = hashGenerationPipeline.run()
    if (processingConfig.processHashComparator) {
      val solrHashDF = solrHashReader.map(r => r.load())
      val hashComparator = HashComparator(solrHashDF, hashList.getOrElse(Seq()), columnNamesConverter)
      dfWithHashes = ProcessingPipeline(dfWithHashes)
        .addProcessor(hashComparator)
        .run()
    }
    val pipeline = ProcessingPipeline(dfWithHashes)
      .setFormatAdjustmentProcessor(formatAdjustmentProcessor)
      .setColumnNamesConverter(Some(columnNamesConverter))
      .addProcessor(MultiLabelClassifier(sparkSession, loadModel(), config.labelsMapping, config.threshold, hashList = hashColumns.getOrElse(Seq())))
    if (processingConfig.thumbnailsEnabled) {
      pipeline.addProcessor(ThumbnailsGenerator(processingConfig.smallThumbnailsSize, processingConfig.mediumThumbnailsSize))
    }
    pipeline.addProcessor(MetadataExtractor())
    analyticsFilters.foreach(pipeline.addFilter)
    pipeline.addProcessor(FlattenMetadataDirectories(processingConfig.allowedDirectories))
    mandatoryTagsFilter.foreach(pipeline.addFilter)
    val extractedDF = pipeline.run()

    writer.write(extractedDF)
  }

}
