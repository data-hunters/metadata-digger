package ai.datahunters.md.pipeline

import ai.datahunters.md.config.BaseConfig
import ai.datahunters.md.listener.OutputFilesCleanupListener
import ai.datahunters.md.processor.{FlattenMetadataDirectories, MetadataExtractor}
import ai.datahunters.md.reader.BasicBinaryFilesReader
import ai.datahunters.md.writer.BasicFileOutputWriter

/**
  * Basic workflow which run the following steps:
  * * Loading binary files from multiple paths
  * * Extracting metadata to embedded column (Map[String, Map[String, String]])
  * * Flattening metadata to achieve remove one level of embedding
  * * In case of CSV file output format - flattening metadata to root level
  * * Saving output to file(s)
  *
  * @param config
  * @param sessionCreator
  */
class BasicExtractionWorkflow(config: BaseConfig, sessionCreator: SessionCreator) extends Workflow {


  override def run(): Unit = {
    val sparkSession = sessionCreator.create()
    if (config.isStandalone()) {
      sparkSession.sparkContext.addSparkListener(new OutputFilesCleanupListener(sparkSession, config.outputDirPath, config.format))
    }
    val rawInputDF = BasicBinaryFilesReader(sparkSession, config.partitionsNum).read(config.inputPaths)

    val extractedDF = ProcessingPipeline(rawInputDF)
      .setFormatAdjustmentProcessor(FormatAdjustmentProcessorFactory(config.format))
      .addProcessor(MetadataExtractor())
      .addProcessor(FlattenMetadataDirectories(config.allowedDirectories))
      .run()

    BasicFileOutputWriter(sparkSession, config.format, config.outputFilesNum).write(extractedDF, config.outputDirPath)
  }




}
