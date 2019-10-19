package ai.datahunters.md.pipeline

import ai.datahunters.md.filter.Filter
import ai.datahunters.md.processor.{ColumnNamesConverter, Processor}
import org.apache.spark.sql.DataFrame

import scala.collection.mutable.ArrayBuffer

/**
  * Run all processing steps.
  *
  * @param inputDF
  */
case class ProcessingPipeline(inputDF: DataFrame) {

  private var formatAdjustmentProcessor: Option[Processor] = None
  private var namesConverter: Option[ColumnNamesConverter] = None
  private val steps = ArrayBuffer[Step]()

  def setFormatAdjustmentProcessor(processor: Option[Processor]): ProcessingPipeline = {
    formatAdjustmentProcessor = processor
    this
  }

  def setColumnNamesConverter(processor: Option[ColumnNamesConverter]): ProcessingPipeline = {
    namesConverter = processor
    this
  }

  def addProcessor(processor: Processor): ProcessingPipeline = {
    steps.append(processor)
    this
  }

  def addFilter(filter: Filter): ProcessingPipeline = {
    steps.append(filter)
    this
  }

  def run(): DataFrame = {
    var bufferDF = inputDF
    for (step <- steps) {
      bufferDF = step.execute(bufferDF)
    }
    bufferDF = formatAdjustmentProcessor.map(_.execute(bufferDF)).getOrElse(bufferDF)
    bufferDF = namesConverter.map(_.execute(bufferDF)).getOrElse(bufferDF)
    bufferDF.printSchema()
    bufferDF
  }

}
