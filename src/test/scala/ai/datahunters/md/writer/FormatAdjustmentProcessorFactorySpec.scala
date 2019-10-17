package ai.datahunters.md.writer

import ai.datahunters.md.UnitSpec
import ai.datahunters.md.processor.FlattenMetadataTags

class FormatAdjustmentProcessorFactorySpec extends UnitSpec {

  "A FormatAdjustmentProcessorFactory" should "create FlattenMetadataTags processor for CSV format" in {
    val processor = FormatAdjustmentProcessorFactory.create("csv")
    assert(processor.isDefined)
    assert(processor.get.isInstanceOf[FlattenMetadataTags])
  }

  it should "return None for JSON format" in {
    val processor = FormatAdjustmentProcessorFactory.create("json")
    assert(processor.isEmpty)
  }

  it should "return None for not supported format" in {
    val processor = FormatAdjustmentProcessorFactory.create("invalid")
    assert(processor.isEmpty)
  }

}
