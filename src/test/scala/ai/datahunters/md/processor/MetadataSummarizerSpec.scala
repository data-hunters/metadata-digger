package ai.datahunters.md.processor

import ai.datahunters.md.schema.BinaryInputSchemaConfig.FilePathCol
import ai.datahunters.md.schema.MetadataSchemaConfig.MetadataContentCol
import ai.datahunters.md.{SparkBaseSpec, UnitSpec}
import ai.datahunters.md.schema.{BinaryInputSchemaConfig, MetadataTagsSchemaConfig}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

class MetadataSummarizerSpec extends UnitSpec with SparkBaseSpec {

  import MetadataSummarizerSpec._

  "A MetadataSummarizer" should "select and sort unique tag names" in {
    val processor = MetadataSummarizer
    val rdd = sparkSession.sparkContext.parallelize(Data)
    val df = sparkSession.createDataFrame(rdd, Schema)
    val outputDF = processor.execute(df)
    val outputFields = outputDF.schema.fields.map(_.name).sorted
    assert(outputFields === Array(MetadataSummarizer.OutputFullTagNameCol) )
    val outputRows = outputDF.collect().map(_.getString(0))
    assert(outputRows === Array("Dir1.tag1", "Dir1.tag2", "Dir2.tag2", "Dir2.tag3", "Dir2.tag4"))
  }
}

object MetadataSummarizerSpec {

  val Data = Seq(
    Row.fromTuple(1, "some/path", Row.fromTuple(Map("tag1" -> "val1"), Map("tag2" -> "val2"))),
    Row.fromTuple(6, "some/path", Row.fromTuple(Map("tag1" -> "val1"), Map("tag4" -> "val4"))),
    Row.fromTuple(3, "some/path", Row.fromTuple(Map("tag2" -> "val1"), Map("tag3" -> "val2")))
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