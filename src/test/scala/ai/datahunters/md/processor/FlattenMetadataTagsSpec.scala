package ai.datahunters.md.processor

import ai.datahunters.md.schema.MetadataTagsSchemaConfig
import ai.datahunters.md.{SparkBaseSpec, UnitSpec}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataTypes, IntegerType, StructField, StructType}

class FlattenMetadataTagsSpec extends UnitSpec with SparkBaseSpec {
  import FlattenMetadataTagsSpec._

  "A FlattenMetadataTags" should "extract embedded tags to root level" in {
    val processor = FlattenMetadataTags()
    val rdd = sparkSession.sparkContext.parallelize(Data)
    val df = sparkSession.createDataFrame(rdd, Schema)
    val outputDF = processor.execute(df)
    val outputFields = outputDF.schema.fields.map(_.name).sorted
    assert(outputFields === Array("Number", "tag1", "tag2", "tag3", "tag4").sorted)
  }

  it should "extract selected embedded tags to root level" in {
    val processor = FlattenMetadataTags(Some(Seq("tag1", "tag2")))
    val rdd = sparkSession.sparkContext.parallelize(Data)
    val df = sparkSession.createDataFrame(rdd, Schema)
    val outputDF = processor.execute(df)
    val outputFields = outputDF.schema.fields.map(_.name).sorted
    assert(outputFields === Array("Number", "tag1", "tag2").sorted)
  }
}

object FlattenMetadataTagsSpec {

  val Data = Seq(
    Row.fromTuple(1, Row.fromTuple(Map("tag1" -> "val1"), Map("tag2" -> "val2"))),
    Row.fromTuple(6, Row.fromTuple(Map("tag1" -> "val1"), Map("tag4" -> "val4"))),
    Row.fromTuple(3, Row.fromTuple(Map("tag2" -> "val1"), Map("tag3" -> "val2")))
  )

  val Schema = StructType(Array(
    StructField("Number", IntegerType, true),
    StructField(MetadataTagsSchemaConfig.MetadataCol, new StructType(Array(
      StructField("Dir1", DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType)),
      StructField("Dir2", DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType))
    )), true)
  )
  )

}