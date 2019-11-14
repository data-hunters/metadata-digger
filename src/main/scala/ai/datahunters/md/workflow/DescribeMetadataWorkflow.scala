package ai.datahunters.md.workflow

import ai.datahunters.md.pipeline.ProcessingPipeline
import ai.datahunters.md.processor.{FlattenMetadataDirectories, MetadataExtractor, MetadataSummarizer}
import ai.datahunters.md.reader.PipelineSource
import ai.datahunters.md.writer.Printer

case class DescribeMetadataWorkflow(reader: PipelineSource) extends Workflow {

  override def run(): Unit = {
    val rawInputDF = reader.load()
    val extractedDF = ProcessingPipeline(rawInputDF)
      .addProcessor(MetadataExtractor())
      .addProcessor(FlattenMetadataDirectories(None))
      .addProcessor(MetadataSummarizer)
      .run()
    Printer.write(extractedDF)
  }
}
