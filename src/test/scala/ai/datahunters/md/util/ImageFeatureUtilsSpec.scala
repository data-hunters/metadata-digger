package ai.datahunters.md.util

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, FileInputStream}
import java.nio.file.{Files, Paths}

import ai.datahunters.md.{SparkBaseSpec, UnitSpec}
import ai.datahunters.md.schema.{BinaryInputSchemaConfig, MultiLabelPredictionSchemaConfig}
import ai.datahunters.md.util.ImageFeatureUtils.JPGType
import com.intel.analytics.bigdl.tensor.Tensor
import com.intel.analytics.bigdl.transform.vision.image.augmentation.Resize
import com.intel.analytics.bigdl.transform.vision.image.{ImageFeature, ImageFrame}
import com.intel.analytics.bigdl.utils.Engine
import com.intel.analytics.zoo.pipeline.api.Net
import javax.imageio.ImageIO
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import scala.io.Source

class ImageFeatureUtilsSpec extends UnitSpec with SparkBaseSpec {

  import ImageFeatureUtilsSpec._

  "A ImageFeatureUtils" should "convert image feature to row using custom keys" in {
    val imgFeature = ImageFeature(Array[Byte]())
    imgFeature.update(ImageFeature.predict, Tensor[Float](Array(1.0f, 0.0f, 0.5f), Array(3)))
    imgFeature.update("id_key", "someId1")
    imgFeature.update(ImageFeature.bytes, Array[Byte]())
    imgFeature.update("base_path_key", "some/path")
    imgFeature.update(ImageFeature.uri, "some/path/file1.jpg")
    val row = ImageFeatureUtils.imageFeatureToRow(ImageFeatureUtilsSpec.LabelsMapping, "id_key", "base_path_key", 0.4f)(imgFeature)
    assert(row.getString(0) === "someId1")
    assert(row.getString(1) === "some/path")
    assert(row.getString(2) === "some/path/file1.jpg")
    assert(row.getAs[Array[Byte]](3) === Array[Byte]())
    assert(row.getAs[Array[String]](4) === Array("person", "chair"))
  }

  it should "convert image feature to row using defaults" in {
    val imgFeature = ImageFeature(Array[Byte]())
    imgFeature.update(ImageFeature.predict, Tensor[Float](Array(1.0f, 0.0f, 0.5f), Array(3)))
    imgFeature.update(BinaryInputSchemaConfig.IDCol, "someId1")
    imgFeature.update(ImageFeature.bytes, Array[Byte]())
    imgFeature.update(BinaryInputSchemaConfig.BasePathCol, "some/path")
    imgFeature.update(ImageFeature.uri, "some/path/file1.jpg")
    val row = ImageFeatureUtils.imageFeatureToRow(ImageFeatureUtilsSpec.LabelsMapping)(imgFeature)
    assert(row.getString(0) === "someId1")
    assert(row.getString(1) === "some/path")
    assert(row.getString(2) === "some/path/file1.jpg")
    assert(row.getAs[Array[Byte]](3) === Array[Byte]())
    assert(row.getAs[Array[String]](4) === Array("person"))
  }

  it should "build ImageFrame from DataFrame using custom schema" in {
    Engine.init
    val fileName = "000000547886_coco_dataset.jpg"
    val filePath = imgPath(fileName)
    val imgFrame = ImageFrame.read(imgPath(""), spark.sparkContext)
    val tupleRow = ("id1", imgPath(""), filePath, Files.readAllBytes(Paths.get(filePath)))
    val input = Seq(
      Row.fromTuple(tupleRow)
    )
    val inputRDD = spark.sparkContext.parallelize(input)
    val schema = StructType(Array[StructField](
      StructField(SampleIdCol, StringType),
      StructField(SampleBasePathCol, StringType),
      StructField(SampleFilePathCol, StringType),
      StructField(SampleFileDataCol, BinaryType)
    ))
    val inputDF = spark.createDataFrame(inputRDD, schema)
    val imageFrame = ImageFeatureUtils.buildImageFrame(inputDF,
      ImageFeatureUtilsSpec.LabelsMapping.size,
      Resize(256, 256),
      "idCol",
    "fileDataCol",
    "basePathCol",
    "filePathCol")
    val imageFeatures = imageFrame.toDistributed().rdd.collect()
    assert(imageFeatures.size === 1)
    val imgFeature = imageFeatures(0)
    assert(imgFeature[String](SampleIdCol) === tupleRow._1)
    assert(imgFeature[String](SampleBasePathCol) === tupleRow._2)
    assert(imgFeature[String](ImageFeature.uri) === tupleRow._3)
    assert(imgFeature[Array[Byte]](ImageFeature.bytes) === tupleRow._4)
  }


  it should "build ImageFrame from DataFrame with default schema and invalid image" in {
    Engine.init
    val fileName = "000000547886_coco_dataset.jpg"
    val filePath = imgPath(fileName)
    val imgFrame = ImageFrame.read(imgPath(""), spark.sparkContext)
    val tupleRow = ("id1", imgPath(""), filePath, Files.readAllBytes(Paths.get(filePath)))
    val invaludTupleRow = ("id2", configPath(""), configPath("empty.config.properties"), Array[Byte]())
    val input = Seq(
      Row.fromTuple(tupleRow),
      Row.fromTuple(invaludTupleRow)
    )
    val inputRDD = spark.sparkContext.parallelize(input)
    val inputDF = spark.createDataFrame(inputRDD, BinaryInputSchemaConfig().schema())
    val imageFrame = ImageFeatureUtils.buildImageFrame(inputDF,
      ImageFeatureUtilsSpec.LabelsMapping.size,
      Resize(256, 256))
    val imageFeatures = imageFrame.toDistributed().rdd.collect()
    assert(imageFeatures.size === 1)
    val imgFeature = imageFeatures(0)
    assert(imgFeature[String](BinaryInputSchemaConfig.IDCol) === tupleRow._1)
    assert(imgFeature[String](BinaryInputSchemaConfig.BasePathCol) === tupleRow._2)
    assert(imgFeature[String](ImageFeature.uri) === tupleRow._3)
    assert(imgFeature[Array[Byte]](ImageFeature.bytes) === tupleRow._4)

  }


  it should "convert gray scaled image to RGB" in {
    val grayBI = ImageIO.read(new FileInputStream(otherImgPath("GRAY_landscape-4518195_960_720_pixabay_license.jpg")))
    assert(grayBI.getData.getNumDataElements === 1)
    val bos = new ByteArrayOutputStream()
    ImageIO.write(grayBI, JPGType, bos)
    val convertedImg = ImageFeatureUtils.convertToRGB(bos.toByteArray, "Image 1")
    val convertedBI = ImageIO.read(new ByteArrayInputStream(convertedImg))
    assert(convertedBI.getData.getNumDataElements === 3)
  }

  it should "convert RGBA image to RGB" in {
    val alphaImgPath = otherImgPath("ALPHA_landscape-4518195_960_720_pixabay_license.png")
    val rgbaBI = ImageIO.read(new FileInputStream(alphaImgPath))
    assert(rgbaBI.getData.getNumDataElements === 4)
    val imgArr = Files.readAllBytes(Paths.get(alphaImgPath))
    val convertedImg = ImageFeatureUtils.convertToRGB(imgArr, "Image 1")
    val convertedBI = ImageIO.read(new ByteArrayInputStream(convertedImg))
    assert(convertedBI.getData.getNumDataElements === 3)
  }
}

object ImageFeatureUtilsSpec {

  val LabelsMapping = Map(
    0 -> "person",
    1 -> "table",
    2 -> "chair"
  )
  val SampleIdCol = "idCol"
  val SampleFileDataCol = "fileDataCol"
  val SampleBasePathCol = "basePathCol"
  val SampleFilePathCol = "filePathCol"
}
