package ai.datahunters.md.pipeline

import ai.datahunters.md.filter.Filter
import ai.datahunters.md.processor.Processor
import org.apache.spark.sql.DataFrame

import scala.collection.mutable.ArrayBuffer

case class ProcessingPipeline(inputDF: DataFrame) {

  private var formatAdjustmentProcessor: Option[Processor] = None
  private val steps = ArrayBuffer[Step]()

  def setFormatAdjustmentProcessor(processor: Option[Processor]): ProcessingPipeline = {
    formatAdjustmentProcessor = processor
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
    bufferDF
  }

}
