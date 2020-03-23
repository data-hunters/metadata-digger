package ai.datahunters.md.workflow

import ai.datahunters.md.config.enrich.MetadataEnrichmentConfig
import ai.datahunters.md.config.processing.ProcessingConfig
import ai.datahunters.md.pipeline.ProcessingPipeline
import ai.datahunters.md.processor.enrich.MultiLabelClassifier
import ai.datahunters.md.processor.{ColumnNamesConverterFactory, DropColumns, Processor}
import ai.datahunters.md.reader.PipelineSource
import ai.datahunters.md.schema.BinaryInputSchemaConfig
import ai.datahunters.md.util.FilesHandler
import ai.datahunters.md.writer.PipelineSink
import com.intel.analytics.bigdl.utils.Engine
import com.intel.analytics.zoo.pipeline.api.Net
import org.apache.spark.sql.SparkSession

/**
  * Run only Metadata Enrichment processors on data
  *
  * @param config
  * @param processingConfig
  * @param sparkSession
  * @param reader
  * @param writer
  * @param formatAdjustmentProcessor
  */
class MetadataEnrichmentWorkflow(config: MetadataEnrichmentConfig,
                                 processingConfig: ProcessingConfig,
                                 sparkSession: SparkSession,
                                 reader: PipelineSource,
                                 writer: PipelineSink,
                                 formatAdjustmentProcessor: Option[Processor]) extends Workflow {

  Engine.init
  private[workflow] val columnNamesConverter = ColumnNamesConverterFactory.create(processingConfig.namingConvention)
  private[workflow] def loadModel() = {
    val finalPath = FilesHandler.sparkFilePath(config.modelFileName)
    Net.load[Float](finalPath)
  }
  private val labelsMapping = config.labelsMapping

  override def run(): Unit = {
    val rawInputDF = reader.load()
    val pipeline = ProcessingPipeline(rawInputDF)
      .setFormatAdjustmentProcessor(formatAdjustmentProcessor)
      .setColumnNamesConverter(Some(columnNamesConverter))
      .addProcessor(MultiLabelClassifier(sparkSession, loadModel(), labelsMapping, config.threshold))
      .addProcessor(DropColumns(Seq(BinaryInputSchemaConfig.FileCol)))
    val extractedDF = pipeline.run()

    writer.write(extractedDF)
  }


}
