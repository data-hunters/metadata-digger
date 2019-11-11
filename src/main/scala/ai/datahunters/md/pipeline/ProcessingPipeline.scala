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

  /**
    * Set Processor that applies all changes required by specific format (e.g. CSV cannot have nested structures).
    *
    * @param processor
    * @return
    */
  def setFormatAdjustmentProcessor(processor: Option[Processor]): ProcessingPipeline = {
    formatAdjustmentProcessor = processor
    this
  }

  /**
    * Set Processor that changes name of all columns to adhere particular naming convention rules.
    * It is executed at the end of whole pipeline after all other processors, including Format Adjustment Processor
    *
    * @param processor
    * @return
    */
  def setColumnNamesConverter(processor: Option[ColumnNamesConverter]): ProcessingPipeline = {
    namesConverter = processor
    this
  }

  /**
    * Add processor to whole processing flow. All processors will be executed in the order of addition.
    *
    * @param processor
    * @return
    */
  def addProcessor(processor: Processor): ProcessingPipeline = {
    steps.append(processor)
    this
  }

  /**
    * Add filter to whole processing flow. It is similar to Processor but its goal is to remove some rows from processing
    * according to specific conditions. It could be applied in every place: before, between and after processors.
    *
    * @param filter
    * @return
    */
  def addFilter(filter: Filter): ProcessingPipeline = {
    steps.append(filter)
    this
  }

  /**
    * Start whole processing pipeline.
    *
    * @return Output DataFrame.
    */
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
