package ai.datahunters.md.processor

import java.io.ByteArrayInputStream
import java.nio.file.{Files, Paths}
import java.util.Base64

import ai.datahunters.md.schema.{BinaryInputSchemaConfig, ThumbnailsSchemaConfig}
import ai.datahunters.md.udf.image.ImageProcessing.ThumbnailSize
import ai.datahunters.md.{SparkBaseSpec, UnitSpec}
import javax.imageio.ImageIO
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

class ThumbnailsGeneratorSpec extends UnitSpec with SparkBaseSpec {

  "A ThumbnailsGenerator" should "make small and medium thumbnails from image in DF" in {
    val generator = ThumbnailsGenerator(Some(ThumbnailSize(300, 200)), Some(ThumbnailSize(800, 600)))
    val inputFile = Files.readAllBytes(Paths.get(imgPath("landscape-4518195_960_720_pixabay_license.jpg")))
    verifyOriginalImg(inputFile)

    val sampleData = Seq(
      Row.fromTuple(("", inputFile))
    )
    val rdd = sparkSession.sparkContext.parallelize(sampleData)
    val schema: StructType = StructType(Array[StructField](
      StructField(BinaryInputSchemaConfig.IDCol, DataTypes.StringType),
      StructField(BinaryInputSchemaConfig.FileCol, DataTypes.BinaryType)
    ))
    val df = sparkSession.createDataFrame(rdd, schema)
    val outputDF = generator.execute(df)
    val output = outputDF.collect()(0)
    val originalThumbFile = output.getAs[Array[Byte]](1)
    verifyOriginalImg(originalThumbFile)
    val smallThumbB64 = output.getAs[String](ThumbnailsSchemaConfig.SmallThumbnailCol)
    val smallThumbFile = Base64.getDecoder().decode(smallThumbB64)
    val smallThumbBI = ImageIO.read(new ByteArrayInputStream(smallThumbFile))
    assert(smallThumbBI.getWidth === 300)
    assert(smallThumbBI.getHeight === 200)
    val mediumThumbB64 = output.getAs[String](ThumbnailsSchemaConfig.MediumThumbnailCol)
    val mediumThumbFile = Base64.getDecoder().decode(mediumThumbB64)
    val mediumThumbBI = ImageIO.read(new ByteArrayInputStream(mediumThumbFile))
    assert(mediumThumbBI.getWidth === 800)
    assert(mediumThumbBI.getHeight === 533)
    assert(output.schema.fields.length === 4)
  }

  it should "make small thumbnail only" in {
    val generator = ThumbnailsGenerator(Some(ThumbnailSize(300, 200)), None)
    val inputFile = Files.readAllBytes(Paths.get(imgPath("landscape-4518195_960_720_pixabay_license.jpg")))
    verifyOriginalImg(inputFile)
    val sampleData = Seq(
      Row.fromTuple(("", inputFile))
    )
    val rdd = sparkSession.sparkContext.parallelize(sampleData)
    val schema: StructType = StructType(Array[StructField](
      StructField(BinaryInputSchemaConfig.IDCol, DataTypes.StringType),
      StructField(BinaryInputSchemaConfig.FileCol, DataTypes.BinaryType)
    ))
    val df = sparkSession.createDataFrame(rdd, schema)
    val outputDF = generator.execute(df)
    val output = outputDF.collect()(0)
    val originalThumbFile = output.getAs[Array[Byte]](1)
    verifyOriginalImg(originalThumbFile)
    val smallThumbB64 = output.getAs[String](ThumbnailsSchemaConfig.SmallThumbnailCol)
    val smallThumbFile = Base64.getDecoder().decode(smallThumbB64)
    val smallThumbBI = ImageIO.read(new ByteArrayInputStream(smallThumbFile))
    assert(smallThumbBI.getWidth === 300)
    assert(smallThumbBI.getHeight === 200)
    assert(output.schema.fields.length === 3)
  }

  it should "make medium thumbnail only" in {
    val generator = ThumbnailsGenerator(None, Some(ThumbnailSize(800, 600)))
    val inputFile = Files.readAllBytes(Paths.get(imgPath("landscape-4518195_960_720_pixabay_license.jpg")))
    verifyOriginalImg(inputFile)
    val sampleData = Seq(
      Row.fromTuple(("", inputFile))
    )
    val rdd = sparkSession.sparkContext.parallelize(sampleData)
    val schema: StructType = StructType(Array[StructField](
      StructField(BinaryInputSchemaConfig.IDCol, DataTypes.StringType),
      StructField(BinaryInputSchemaConfig.FileCol, DataTypes.BinaryType)
    ))
    val df = sparkSession.createDataFrame(rdd, schema)
    val outputDF = generator.execute(df)
    val output = outputDF.collect()(0)
    val originalThumbFile = output.getAs[Array[Byte]](1)
    verifyOriginalImg(originalThumbFile)
    val mediumThumbB64 = output.getAs[String](ThumbnailsSchemaConfig.MediumThumbnailCol)
    val mediumThumbFile = Base64.getDecoder().decode(mediumThumbB64)
    val mediumThumbBI = ImageIO.read(new ByteArrayInputStream(mediumThumbFile))
    assert(mediumThumbBI.getWidth === 800)
    assert(mediumThumbBI.getHeight === 533)
    assert(output.schema.fields.length === 3)
  }

  def verifyOriginalImg(inputFile: Array[Byte]): Unit = {
    val inputBI = ImageIO.read(new ByteArrayInputStream(inputFile))
    assert(inputBI.getWidth === 960)
    assert(inputBI.getHeight === 639)
  }
}

