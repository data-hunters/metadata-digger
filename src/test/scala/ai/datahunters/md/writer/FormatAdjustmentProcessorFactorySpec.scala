package ai.datahunters.md.writer

import ai.datahunters.md.UnitSpec
import ai.datahunters.md.config.enrich.MetadataEnrichmentConfig
import ai.datahunters.md.config.processing.ProcessingConfig
import ai.datahunters.md.processor.{FlattenArrays, FlattenMetadataTags}
import org.mockito.Mockito._

class FormatAdjustmentProcessorFactorySpec extends UnitSpec {

  "A FormatAdjustmentProcessorFactory" should "create FlattenMetadataTags processor for CSV format" in {
    val config = mock[ProcessingConfig]
    when(config.outputFormat).thenReturn("csv")
    val processor = FormatAdjustmentProcessorFactory.create(config)
    assert(processor.isDefined)
    assert(processor.get.isInstanceOf[FlattenMetadataTags])
  }

  it should "return FlattenMetadataTags processor for Solr format" in {
    val config = mock[ProcessingConfig]
    when(config.outputFormat).thenReturn("solr")
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

  it should "return FlattenArrays processor for enrichment processing and CSV format" in {
    val enrichmentConfig = mock[MetadataEnrichmentConfig]
    when(enrichmentConfig.labelsMapping).thenReturn(Map[Int, String]())
    when(enrichmentConfig.labelsOutputDelimiter).thenReturn(",")
    val processor = FormatAdjustmentProcessorFactory.create(enrichmentConfig, "csv")
    assert(processor.isDefined)
    assert(processor.get.isInstanceOf[FlattenArrays])
  }

  it should "return None for enrichment processing and JSON format" in {
    val enrichmentConfig = mock[MetadataEnrichmentConfig]
    val processor = FormatAdjustmentProcessorFactory.create(enrichmentConfig, "json")
    assert(processor.isEmpty)
  }

  it should "return None for enrichment processing and Solr format" in {
    val enrichmentConfig = mock[MetadataEnrichmentConfig]
    val processor = FormatAdjustmentProcessorFactory.create(enrichmentConfig, "solr")
    assert(processor.isEmpty)
  }

  it should "return None for enrichment processing and not supported format" in {
    val enrichmentConfig = mock[MetadataEnrichmentConfig]
    val processor = FormatAdjustmentProcessorFactory.create(enrichmentConfig, "invalid")
    assert(processor.isEmpty)
  }



}
