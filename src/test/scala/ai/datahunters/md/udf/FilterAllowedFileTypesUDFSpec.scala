package ai.datahunters.md.udf

import ai.datahunters.md.schema.{BinaryInputSchemaConfig, EmbeddedMetadataSchemaConfig}
import ai.datahunters.md.{SparkBaseSpec, UnitSpec}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.functions._
import EmbeddedMetadataSchemaConfig._

class FilterAllowedFileTypesUDFSpec extends UnitSpec with SparkBaseSpec{
  import FilterAllowedFileTypesUDFSpec._

  "filterAllowedFileTypesUDF" should "return empty DF due to allowed file types missing" in {
    val testedUDF = Filters.filterAllowedFileTypesUDF(Seq())
    val rdd = sparkSession.sparkContext.parallelize(Data)
    val df = sparkSession.createDataFrame(rdd, Schema)
    val result = df.filter(testedUDF(col(FileTypeCol)))
    assert(result.count() === 0)
  }

  it should "return empty DF" in {
    val testedUDF = Filters.filterAllowedFileTypesUDF(Seq(FileType3))
    val rdd = sparkSession.sparkContext.parallelize(Data)
    val df = sparkSession.createDataFrame(rdd, Schema)
    val result = df.filter(testedUDF(col(FileTypeCol)))
    assert(result.count() === 0)
  }

  it should "return one row" in {
    val testedUDF = Filters.filterAllowedFileTypesUDF(Seq(FileType1, FileType3))
    val rdd = sparkSession.sparkContext.parallelize(Data)
    val df = sparkSession.createDataFrame(rdd, Schema)
    val result = df.filter(testedUDF(col(FileTypeCol)))
    assert(result.count() === 1)
    val row1 = result.collect()(0)
    val fileTypeField = row1.get(row1.fieldIndex(FileTypeCol))
    assert(FileType1 === fileTypeField)
  }

  it should "return all rows" in {
    val testedUDF = Filters.filterAllowedFileTypesUDF(Seq(FileType1, FileType2))
    val rdd = sparkSession.sparkContext.parallelize(Data)
    val df = sparkSession.createDataFrame(rdd, Schema)
    val result = df.filter(testedUDF(col(FileTypeCol)))
    assert(result.count() === 2)
    val row1 = result.collect()(0)
    val row2 = result.collect()(1)
    val fileTypeField1 = row1.get(row1.fieldIndex(FileTypeCol))
    val fileTypeField2 = row2.get(row2.fieldIndex(FileTypeCol))
    assert(FileType1 === fileTypeField1)
    assert(FileType2 === fileTypeField2)
  }

}

object FilterAllowedFileTypesUDFSpec {

  val Id1 = "id1"
  val Id2 = "id2"
  val Path1 = "some/random/path/1"
  val Path2 = "some/random/path/2"
  val FileType1 = "fileType1"
  val FileType2 = "fileType2"
  val FileType3 = "fileType3"
  val TagCount1 = 3
  val TagCount2 = 2
  val Dir1Name = "dir1"
  val Dir2Name = "dir2"
  val Tag1Name = "tag1"
  val Tag2Name = "tag2"
  val Tag3Name = "tag3"
  val Tag4Name = "tag4"
  val Value1 = "val1"
  val Value2 = "val2"
  val Value3 = "val3"
  val Value4 = "val4"

  val Data = Seq(
    Row.fromSeq(Seq(Id1, Path1, FileType1, Row.fromSeq(Seq(TagCount1, Map(Dir1Name -> Map(Tag1Name -> Value1),
      Dir2Name -> Map(Tag2Name -> Value2, Tag3Name -> Value3)))), Seq(Dir1Name, Dir2Name), Seq(Tag1Name, Tag2Name, Tag3Name))),
    Row.fromSeq(Seq(Id2, Path2, FileType2, Row.fromSeq(Seq(TagCount2, Map(Dir1Name -> Map(Tag2Name -> Value2),
      Dir2Name -> Map(Tag4Name -> Value4)))), Seq(Dir1Name, Dir2Name), Seq(Tag2Name, Tag4Name)))
  )

  val Schema: StructType = StructType(
    Array(
      StructField(BinaryInputSchemaConfig.IDCol, DataTypes.StringType),
      StructField(BinaryInputSchemaConfig.BasePathCol, DataTypes.StringType),
      StructField(FileTypeCol, DataTypes.StringType),
      StructField(MetadataCol, StructType(
        Array(
          StructField(TagsCountCol, DataTypes.IntegerType),
          StructField(TagsCol, DataTypes.createMapType(DataTypes.StringType, DataTypes.createMapType(DataTypes.StringType,
            DataTypes.StringType)))
        )
      )),
      StructField(DirectoryNamesCol, DataTypes.createArrayType(DataTypes.StringType)),
      StructField(TagNamesCol, DataTypes.createArrayType(DataTypes.StringType))
    )
  )

}