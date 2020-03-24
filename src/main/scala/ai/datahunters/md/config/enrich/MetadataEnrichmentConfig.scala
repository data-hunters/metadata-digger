package ai.datahunters.md.config.enrich

import java.nio.file.Paths

import ai.datahunters.md.config.ConfigLoader.assignDefaults
import ai.datahunters.md.config.ConfigLoader.ListElementsDelimiter
import ai.datahunters.md.config.GeneralConfig
import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

case class MetadataEnrichmentConfig(labelsMapping: Map[Int, String],
                                    threshold: Float,
                                    modelPath: String,
                                    outputLabelsDelimiter: String) extends GeneralConfig {

  import MetadataEnrichmentConfig._

  val modelFileName = Paths.get(modelPath)
    .getFileName
    .toString

  override def adjustSparkConfig(sparkSession: SparkSession): Unit = {
    Logger.info(s"Adding model ${modelPath}")
    sparkSession.sparkContext
      .addFile(modelPath)
  }
}

object MetadataEnrichmentConfig {

  val Logger = LoggerFactory.getLogger(classOf[MetadataEnrichmentConfig])

  val LabelsMappingKey = "enrichment.classifier.mapping"
  val PredictionThresholdKey = "enrichment.classifier.threshold"
  val ModelPathKey = "enrichment.classifier.modelPath"
  val OutputLabelsDelimiterKey = "enrichment.output.labelsDelimiter"

  val Defaults = Map(
    PredictionThresholdKey -> 0.5f,
    OutputLabelsDelimiterKey -> ","
  )

  val MappingDelimiter = ":"

  def build(config: Config): MetadataEnrichmentConfig = {
    val configWithDefaults = assignDefaults(config, Defaults)
    MetadataEnrichmentConfig(
      parse(configWithDefaults.getString(LabelsMappingKey)),
      configWithDefaults.getDouble(PredictionThresholdKey).toFloat,
      configWithDefaults.getString(ModelPathKey),
      configWithDefaults.getString(OutputLabelsDelimiterKey)
    )
  }

  protected def parse(mappings: String): Map[Int, String] = {
    mappings.split(ListElementsDelimiter)
      .map(_.split(MappingDelimiter))
      .map(mapping => (mapping(0).toInt -> mapping(1)))
      .toMap
  }
}
