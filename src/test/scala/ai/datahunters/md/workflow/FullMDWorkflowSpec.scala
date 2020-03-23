package ai.datahunters.md.workflow

import java.nio.file.{Files, Paths}

import ai.datahunters.md.config.enrich.MetadataEnrichmentConfig
import ai.datahunters.md.config.processing.ProcessingConfig
import ai.datahunters.md.processor.Processor
import ai.datahunters.md.reader.PipelineSource
import ai.datahunters.md.schema.BinaryInputSchemaConfig
import ai.datahunters.md.writer.PipelineSink
import ai.datahunters.md.{SparkBaseSpec, UnitSpec}
import org.apache.spark.sql.Row
import org.mockito.ArgumentMatchers.any
import org.mockito.InOrder
import org.mockito.Mockito.when

class FullMDWorkflowSpec extends UnitSpec with SparkBaseSpec {

  import FullMDWorkflowSpec._


  "A FullMDWorkflow" should "run all elements in appropriate order" in {
    val imgBytes = Files.readAllBytes(Paths.get(imgPath(ImagePath)))
    val inputData = Seq(
      Row.fromTuple("somehash", "some/path", "some/path/img.jpg", imgBytes)
    )
    val rdd = sparkSession.sparkContext.parallelize(inputData)
    sparkSession.sparkContext.addFile(modelPath("lenet_based"))
    val readerOutputDF = sparkSession.createDataFrame(rdd, BinaryInputSchemaConfig().schema())
    val processingConfig = mock[ProcessingConfig]
    when(processingConfig.namingConvention).thenReturn("snakeCase")
    when(processingConfig.allowedDirectories).thenReturn(None)
    when(processingConfig.mandatoryTags).thenReturn(None)
    val enrichmentConfig = mock[MetadataEnrichmentConfig]
    when(enrichmentConfig.threshold).thenReturn(0.5f)
    when(enrichmentConfig.outputLabelsDelimiter).thenReturn(",")
    when(enrichmentConfig.modelPath).thenReturn(modelPath("lenet_based"))
    when(enrichmentConfig.labelsMapping).thenReturn(Map(0 -> "label1", 1 -> "label2"))
    when(enrichmentConfig.modelFileName).thenReturn("lenet_based")
    val reader = mock[PipelineSource]
    when(reader.load()).thenReturn(readerOutputDF)
    val writer = mock[PipelineSink]
    val formatAdjustmentProcessor = mock[Processor]
    when(formatAdjustmentProcessor.execute(any())).thenReturn(readerOutputDF)
    val workflow = new FullMDWorkflow(enrichmentConfig, processingConfig, sparkSession, reader, writer, Some(formatAdjustmentProcessor))
    workflow.run()
    val inOrderExecution: InOrder = org.mockito.Mockito.inOrder(reader, formatAdjustmentProcessor, writer)
    inOrderExecution.verify(reader).load()
    inOrderExecution.verify(formatAdjustmentProcessor).execute(any())
    inOrderExecution.verify(writer).write(any())
  }
}

object FullMDWorkflowSpec {

  val ImagePath = "landscape-4518195_960_720_pixabay_license.jpg"
}
