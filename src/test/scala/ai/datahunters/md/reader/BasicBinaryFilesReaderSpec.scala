package ai.datahunters.md.reader

import java.nio.file.{Files, Paths}

import ai.datahunters.md.schema.BinaryInputSchemaConfig
import ai.datahunters.md.{SparkBaseSpec, UnitSpec}
import org.apache.spark.sql.DataFrame

class BasicBinaryFilesReaderSpec extends UnitSpec with SparkBaseSpec {
  import BinaryInputSchemaConfig._
  import BinaryFilesReader._


  "An BasicBinaryFilesReader" should "properly load image file to dataframe" in {
    val reader = BasicBinaryFilesReader(sparkSession, 1, Seq(imgPath("")))
    val df = reader.load()
    verifyReaderResults(df)
  }

  it should "properly load image file to dataframe from list of paths" in {
    val reader = BasicBinaryFilesReader(sparkSession, 1, Seq(imgPath("")))
    val df = reader.load()
    verifyReaderResults(df)
  }

  private def verifyReaderResults(df: DataFrame): Unit = {
    val expectedFilePath = imgPath("landscape-4518195_960_720_pixabay_license.jpg")
    val fields = df.schema.fields.map(_.name)
    assert(fields === Array(ContentHash, BasePathCol, FilePathCol, FileCol))
    val row = df.collect()(0)
    val expectedContentHash: String = md5sum(imgPath(""))
    val file: Array[Byte] = row.getAs(FileCol)
    val filePath = row.getAs[String](FilePathCol)
    val expectedFile = Files.readAllBytes(Paths.get(expectedFilePath))
    assert(imgPath("") === row.getAs[String](BasePathCol))
    assert(filePath.endsWith(expectedFilePath))
    assert(file === expectedFile)
  }

}
