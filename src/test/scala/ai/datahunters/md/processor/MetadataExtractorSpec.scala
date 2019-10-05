package ai.datahunters.md.processor

import java.nio.file.{Files, Paths}

import ai.datahunters.md.schema.{BinaryInputSchemaConfig, EmbeddedMetadataSchemaConfig}
import ai.datahunters.md.{SparkBaseSpec, UnitSpec}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

class MetadataExtractorSpec extends UnitSpec with SparkBaseSpec {
  import MetadataExtractorSpec._

  "A MetadataExtractor" should "extract metadata from image" in {
    val imgBytes = Files.readAllBytes(Paths.get(imgPath(ImagePath)))
    val data = Seq(
      Row.fromTuple("some/path", imgBytes)
    )
    val rdd = sparkSession.sparkContext.parallelize(data)
    val df = sparkSession.createDataFrame(rdd, Schema)
    val processor = MetadataExtractor()
    val outputDF = processor.execute(df)
    val fields = outputDF.schema.fields
    assert(!fields.contains(BinaryInputSchemaConfig.FileCol))
    val row = outputDF.collect()(0)
    val metadata = row.getStruct(row.fieldIndex(EmbeddedMetadataSchemaConfig.MetadataCol))
    val tagsCount = metadata.getAs[Int](EmbeddedMetadataSchemaConfig.TagsCountCol)
    val tags = metadata.getMap[String, Map[String, String]](metadata.fieldIndex(EmbeddedMetadataSchemaConfig.TagsCol))
    val dirs = metadata.getAs[Seq[String]](EmbeddedMetadataSchemaConfig.DirectoriesCol)

    val expectedTagPair = ("Component3", "Cr component: Quantization table 1, Sampling factors 1 horiz/1 vert")
    val jpegDir = tags.get("JPEG").get
    assert(jpegDir.get(expectedTagPair._1).get === expectedTagPair._2)
    assert(tagsCount === 19)
    assert(ExpectedDirs === dirs)
  }

}

object MetadataExtractorSpec {

  val ImagePath = "landscape-4518195_960_720_pixabay_license.jpg"
  val ExpectedDirs = Seq(
    "JPEG",
    "JFIF",
    "Exif IFD0",
    "Huffman",
    "File Type"
  )

  val Schema = StructType(Array(
    StructField("Path", DataTypes.StringType, true),
    StructField(BinaryInputSchemaConfig.FileCol, DataTypes.BinaryType, true)
  )
  )

}