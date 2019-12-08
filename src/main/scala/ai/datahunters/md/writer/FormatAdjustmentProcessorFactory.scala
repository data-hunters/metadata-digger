package ai.datahunters.md.writer

import ai.datahunters.md.config.processing.ProcessingConfig
import ai.datahunters.md.processor.{FlattenMetadataTags, Processor}

object FormatAdjustmentProcessorFactory {

  def create(config: ProcessingConfig): Option[Processor] = config.outputFormat match {
    case FileOutputWriter.CsvFormat => Some(new FlattenMetadataTags(config.metadataColumnsPrefix, config.includeDirsInTags, removeArrays = true))
    case FileOutputWriter.JsonFormat => None
    case SolrWriter.FormatName => Some(FlattenMetadataTags(config.metadataColumnsPrefix,
      includeDirName = config.includeDirsInTags,
      includeMetadataContent = config.includeMetadataContent))
    case other => None
  }

}
