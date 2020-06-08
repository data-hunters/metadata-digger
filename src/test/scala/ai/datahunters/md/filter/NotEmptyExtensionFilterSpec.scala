package ai.datahunters.md.filter

import ai.datahunters.md.schema.{BinaryInputSchemaConfig, EmbeddedMetadataSchemaConfig, MetadataTagsSchemaConfig}
import ai.datahunters.md.{SparkBaseSpec, UnitSpec}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

class NotEmptyExtensionFilterSpec extends UnitSpec with SparkBaseSpec {
  import NotEmptyExtensionFilterSpec._

  "A NotEmptyExtensionFilter" should "not pass any row due to lack of allowed extension" in {
    val filter = new NotEmptyExtensionFilter(Seq[String]())
    val rdd = sparkSession.sparkContext.parallelize(Data)
    val df = sparkSession.createDataFrame(rdd, Schema)
    val outputDF = filter.execute(df)
    val collectedOutput = outputDF.collect()
    val outputFields = outputDF.schema.fields.map(_.name)
    assert( Array(BinaryInputSchemaConfig.IDCol, BinaryInputSchemaConfig.BasePathCol, EmbeddedMetadataSchemaConfig.FileTypeCol,
      MetadataTagsSchemaConfig.MetadataCol, EmbeddedMetadataSchemaConfig.DirectoryNamesCol, EmbeddedMetadataSchemaConfig.TagNamesCol) === outputFields)
    assert(collectedOutput.length === 0)
  }

  it should "pass one row" in {
    val filter = new NotEmptyExtensionFilter(Seq[String](FileType1, FileType3))
    val rdd = sparkSession.sparkContext.parallelize(Data)
    val df = sparkSession.createDataFrame(rdd, Schema)
    val outputDF = filter.execute(df)
    val collectedOutput = outputDF.collect()
    val outputFields = outputDF.schema.fields.map(_.name)
    assert( Array(BinaryInputSchemaConfig.IDCol, BinaryInputSchemaConfig.BasePathCol, EmbeddedMetadataSchemaConfig.FileTypeCol,
      MetadataTagsSchemaConfig.MetadataCol, EmbeddedMetadataSchemaConfig.DirectoryNamesCol, EmbeddedMetadataSchemaConfig.TagNamesCol) === outputFields)
    assert(collectedOutput.length === 1)
    val row1 = outputDF.collect()(0)
    val fileTypeField = row1.get(row1.fieldIndex(EmbeddedMetadataSchemaConfig.FileTypeCol))
    assert(FileType1 === fileTypeField)
  }

  it should "pass all rows" in {
    val filter = new NotEmptyExtensionFilter(Seq[String](FileType1, FileType2))
    val rdd = sparkSession.sparkContext.parallelize(Data)
    val df = sparkSession.createDataFrame(rdd, Schema)
    val outputDF = filter.execute(df)
    val collectedOutput = outputDF.collect()
    val outputFields = outputDF.schema.fields.map(_.name)
    assert( Array(BinaryInputSchemaConfig.IDCol, BinaryInputSchemaConfig.BasePathCol, EmbeddedMetadataSchemaConfig.FileTypeCol,
      MetadataTagsSchemaConfig.MetadataCol, EmbeddedMetadataSchemaConfig.DirectoryNamesCol, EmbeddedMetadataSchemaConfig.TagNamesCol) === outputFields)
    assert(collectedOutput.length === 2)
    val row1 = outputDF.collect()(0)
    val row2 = outputDF.collect()(1)
    val fileTypeField1 = row1.get(row1.fieldIndex(EmbeddedMetadataSchemaConfig.FileTypeCol))
    val fileTypeField2 = row2.get(row2.fieldIndex(EmbeddedMetadataSchemaConfig.FileTypeCol))
    assert(FileType1 === fileTypeField1)
    assert(FileType2 === fileTypeField2)
  }

  it should "pass no rows" in {
    val filter = new NotEmptyExtensionFilter(Seq[String](FileType3))
    val rdd = sparkSession.sparkContext.parallelize(Data)
    val df = sparkSession.createDataFrame(rdd, Schema)
    val outputDF = filter.execute(df)
    val collectedOutput = outputDF.collect()
    val outputFields = outputDF.schema.fields.map(_.name)
    assert( Array(BinaryInputSchemaConfig.IDCol, BinaryInputSchemaConfig.BasePathCol, EmbeddedMetadataSchemaConfig.FileTypeCol,
      MetadataTagsSchemaConfig.MetadataCol, EmbeddedMetadataSchemaConfig.DirectoryNamesCol, EmbeddedMetadataSchemaConfig.TagNamesCol) === outputFields)
    assert(collectedOutput.length === 0)
  }

}

object NotEmptyExtensionFilterSpec {

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
      StructField(EmbeddedMetadataSchemaConfig.FileTypeCol, DataTypes.StringType),
      StructField(MetadataTagsSchemaConfig.MetadataCol, StructType(
        Array(
          StructField(EmbeddedMetadataSchemaConfig.TagsCountCol, DataTypes.IntegerType),
          StructField(EmbeddedMetadataSchemaConfig.TagsCol, DataTypes.createMapType(DataTypes.StringType, DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType)))
        )
      )),
      StructField(EmbeddedMetadataSchemaConfig.DirectoryNamesCol, DataTypes.createArrayType(DataTypes.StringType)),
      StructField(EmbeddedMetadataSchemaConfig.TagNamesCol, DataTypes.createArrayType(DataTypes.StringType))
    )
  )

}