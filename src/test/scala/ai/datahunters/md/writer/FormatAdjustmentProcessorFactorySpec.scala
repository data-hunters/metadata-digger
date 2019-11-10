package ai.datahunters.md.writer

import ai.datahunters.md.UnitSpec
import ai.datahunters.md.config.processing.ProcessingConfig
import ai.datahunters.md.processor.FlattenMetadataTags
import org.mockito.Mockito._

class FormatAdjustmentProcessorFactorySpec extends UnitSpec {

  "A FormatAdjustmentProcessorFactory" should "create FlattenMetadataTags processor for CSV format" in {
    val config = mock[ProcessingConfig]
    when(config.outputFormat).thenReturn("csv")
    val processor = FormatAdjustmentProcessorFactory.create(config)
    assert(processor.isDefined)
    assert(processor.get.isInstanceOf[FlattenMetadataTags])
  }

  it should "return None for JSON format" in {
    val config = mock[ProcessingConfig]
    when(config.outputFormat).thenReturn("json")
    val processor = FormatAdjustmentProcessorFactory.create(config)
    assert(processor.isEmpty)
  }

  it should "return None for not supported format" in {
    val config = mock[ProcessingConfig]
    when(config.outputFormat).thenReturn("invalid")
    val processor = FormatAdjustmentProcessorFactory.create(config)
    assert(processor.isEmpty)
  }

}
