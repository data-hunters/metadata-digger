package ai.datahunters.md.processor
import ai.datahunters.md.schema.{BinaryInputSchemaConfig, ThumbnailsSchemaConfig}
import ai.datahunters.md.udf.image.ImageProcessing
import ai.datahunters.md.udf.image.ImageProcessing.ThumbnailSize
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
  * Generates two additional columns with small and medium thumbnail.
  *
  * @param small
  * @param medium
  */
case class ThumbnailsGenerator(small: Option[ThumbnailSize], medium: Option[ThumbnailSize]) extends Processor {

  import ImageProcessing.generateThumbnail

  override def execute(inputDF: DataFrame): DataFrame = {
    val afterSmallGen = small.map(ss => {
      inputDF.withColumn(ThumbnailsSchemaConfig.SmallThumbnailCol, generateThumbnail(ss)(col(BinaryInputSchemaConfig.FileCol)))
    }).getOrElse(inputDF)
    medium.map(ms => {
      afterSmallGen.withColumn(ThumbnailsSchemaConfig.MediumThumbnailCol, generateThumbnail(ms)(col(BinaryInputSchemaConfig.FileCol)))
    }).getOrElse(afterSmallGen)
  }

}


