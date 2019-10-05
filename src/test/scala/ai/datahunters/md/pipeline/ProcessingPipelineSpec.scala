package ai.datahunters.md.pipeline

import ai.datahunters.md.UnitSpec
import ai.datahunters.md.filter.Filter
import ai.datahunters.md.processor.Processor
import org.apache.spark.sql.DataFrame
import org.mockito.InOrder
import org.mockito.Mockito._

class ProcessingPipelineSpec extends UnitSpec {

  "A ProcessingPipeline" should "call processors in the right order" in {
    val df = mock[DataFrame]
    val processor1 = mock[Processor]
    val processor2 = mock[Processor]
    val processor3 = mock[Processor]
    val filter1 = mock[Filter]
    val filter2 = mock[Filter]
    val formatAdjustmentProcessor = mock[Processor]
    when(processor1.execute(df)).thenReturn(df)
    when(processor2.execute(df)).thenReturn(df)
    when(processor3.execute(df)).thenReturn(df)
    when(filter1.execute(df)).thenReturn(df)
    when(filter2.execute(df)).thenReturn(df)
    when(formatAdjustmentProcessor.execute(df)).thenReturn(df)
    val pipeline = ProcessingPipeline(df)
      .setFormatAdjustmentProcessor(Some(formatAdjustmentProcessor))
      .addProcessor(processor2)
      .addFilter(filter2)
      .addProcessor(processor1)
      .addFilter(filter1)
      .addProcessor(processor3)
    val outputDF = pipeline.run()
    assert(outputDF === df)
    val inOrderExecution: InOrder = org.mockito.Mockito.inOrder(processor2, filter2, processor1, filter1, processor3, formatAdjustmentProcessor)
    inOrderExecution.verify(processor2).execute(df)
    inOrderExecution.verify(filter2).execute(df)
    inOrderExecution.verify(processor1).execute(df)
    inOrderExecution.verify(filter1).execute(df)
    inOrderExecution.verify(processor3).execute(df)
    inOrderExecution.verify(formatAdjustmentProcessor).execute(df)
    inOrderExecution.verifyNoMoreInteractions()
  }

  it should "properly run all processors when FormatAdjustmentProcessor is not available" in {
    val df = mock[DataFrame]
    val processor1 = mock[Processor]
    val processor2 = mock[Processor]
    val filter1 = mock[Filter]

    when(processor1.execute(df)).thenReturn(df)
    when(processor2.execute(df)).thenReturn(df)
    when(filter1.execute(df)).thenReturn(df)
    val pipeline = ProcessingPipeline(df)
      .addProcessor(processor2)
      .addProcessor(processor1)
      .addFilter(filter1)
    val outputDF = pipeline.run()
    assert(outputDF === df)
    val inOrderExecution: InOrder = org.mockito.Mockito.inOrder(processor2, processor1, filter1)
    inOrderExecution.verify(processor2).execute(df)
    inOrderExecution.verify(processor1).execute(df)
    inOrderExecution.verify(filter1).execute(df)
    inOrderExecution.verifyNoMoreInteractions()
  }
}
