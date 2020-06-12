package ai.datahunters.md.writer

import ai.datahunters.md.config.enrich.MetadataEnrichmentConfig
import ai.datahunters.md.config.processing.ProcessingConfig
import ai.datahunters.md.processor.{FlattenArrays, FlattenMetadataTags, Processor, SolrAdjustments}
import ai.datahunters.md.schema.MultiLabelPredictionSchemaConfig

object FormatAdjustmentProcessorFactory {

  def create(config: ProcessingConfig): Option[Processor] = config.outputFormat match {
    case FileOutputWriter.CsvFormat => Some(new FlattenMetadataTags(config.metadataColumnsPrefix, config.includeDirsInTags, flattenArrays = true, removeArrays = true))
    case FileOutputWriter.JsonFormat => None
    case SolrWriter.FormatName =>
      Some(SolrAdjustments(config.metadataColumnsPrefix,
      includeDirName = config.includeDirsInTags,
      includeMetadataContent = config.includeMetadataContent))
    case other => None
  }

  def create(config: MetadataEnrichmentConfig, outputFormat: String): Option[Processor] = outputFormat match {
    case FileOutputWriter.CsvFormat => Some(new FlattenArrays(config.outputLabelsDelimiter)(Seq(MultiLabelPredictionSchemaConfig().LabelsCol)))
    case FileOutputWriter.JsonFormat => None
    case SolrWriter.FormatName => None
    case other => None
  }


}
