package ai.datahunters.md.pipeline

import ai.datahunters.md.processor.{FlattenMetadataTags, Processor}
import ai.datahunters.md.writer.FileOutputWriter

object FormatAdjustmentProcessorFactory {

  def apply(format: String): Option[Processor] = format match {
    case FileOutputWriter.CsvFormat => Some(FlattenMetadataTags())
    case FileOutputWriter.JsonFormat => None
    case _ => None
  }
}
