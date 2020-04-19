package ai.datahunters.md.processor

import ai.datahunters.md.processor.FlattenMetadataTags.TheSameTagNamesException
import ai.datahunters.md.schema.MetadataSchemaConfig.MetadataContentCol
import ai.datahunters.md.schema.{BinaryInputSchemaConfig, MetadataTagsSchemaConfig}
import ai.datahunters.md.{SparkBaseSpec, UnitSpec}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

class FlattenMetadataTagsSpec extends UnitSpec with SparkBaseSpec {
  import FlattenMetadataTagsSpec._
  import BinaryInputSchemaConfig._

  "A FlattenMetadataTags" should "extract embedded tags to root level" in {
    val processor = FlattenMetadataTags("", includeMetadataContent = true)
    val rdd = sparkSession.sparkContext.parallelize(Data)
    val df = sparkSession.createDataFrame(rdd, Schema)
    val outputDF = processor.execute(df)
    val outputFields = outputDF.schema.fields.map(_.name).sorted
    assert( Array(FilePathCol, MetadataContentCol, "Number", "tag1", "tag2", "tag3", "tag4").sorted === outputFields)
  }

  it should "extract selected embedded tags to root level" in {
    val processor = FlattenMetadataTags("", false, true, true, Some(Seq("tag1", "tag2")))
    val rdd = sparkSession.sparkContext.parallelize(Data)
    val df = sparkSession.createDataFrame(rdd, Schema)
    val outputDF = processor.execute(df)
    val outputFields = outputDF.schema.fields.map(_.name).sorted
    assert(Array(FilePathCol, MetadataContentCol, "Number", "tag1", "tag2").sorted === outputFields)
  }

  it should "throw exception in case of the same tag names" in {
    val processor = FlattenMetadataTags("", false, true)
    val rdd = sparkSession.sparkContext.parallelize(InvalidData)
    val df = sparkSession.createDataFrame(rdd, Schema)
    intercept[TheSameTagNamesException] {
      processor.execute(df)
    }
  }
}

object FlattenMetadataTagsSpec {

  val Data = Seq(
    Row.fromTuple(1, "some/path", Row.fromTuple(Map("tag1" -> "val1"), Map("tag2" -> "val2"))),
    Row.fromTuple(6, "some/path", Row.fromTuple(Map("tag1" -> "val1"), Map("tag4" -> "val4"))),
    Row.fromTuple(3, "some/path", Row.fromTuple(Map("tag2" -> "val1"), Map("tag3" -> "val2")))
  )

  val InvalidData = Seq(
    Row.fromTuple(6, "some/path", Row.fromTuple(Map("tag1" -> "val1"), Map("Tag1" -> "val1")))
  )

  val Schema = StructType(Array(
    StructField("Number", IntegerType, true),
    StructField(BinaryInputSchemaConfig.FilePathCol, StringType, true),
    StructField(MetadataTagsSchemaConfig.MetadataCol, new StructType(Array(
      StructField("Dir1", DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType)),
      StructField("Dir2", DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType))
    )), true)
  )
  )

}