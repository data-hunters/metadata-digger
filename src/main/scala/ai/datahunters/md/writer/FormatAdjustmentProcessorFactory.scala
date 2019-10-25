package ai.datahunters.md.writer

import ai.datahunters.md.processor.{FlattenMetadataTags, Processor}

object FormatAdjustmentProcessorFactory {

  def create(format: String, includeDirsInTags: Boolean = false, metadataColPrefix: String = ""): Option[Processor] = format match {
    case FileOutputWriter.CsvFormat => Some(new FlattenMetadataTags(metadataColPrefix, includeDirsInTags, removeArrays = true))
    case FileOutputWriter.JsonFormat => None
    case SolrWriter.FormatName => Some(FlattenMetadataTags(metadataColPrefix, includeDirsInTags))
    case _ => None
  }
}
