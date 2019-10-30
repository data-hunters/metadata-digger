package ai.datahunters.md.processor

import ai.datahunters.md.{SparkBaseSpec, UnitSpec}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataTypes, IntegerType, StructField, StructType}

class FlattenColumnSpec extends UnitSpec with SparkBaseSpec {
  import FlattenColumnSpec._

  "A FlattenColumn" should "move all embedded column on root level in schema" in {
    val processor = FlattenColumn("SomeEmbeddedCol")
    val rdd = sparkSession.sparkContext.parallelize(Data)
    val df = sparkSession.createDataFrame(rdd, Schema)
    val outputDF = processor.execute(df)
    val outputFields = outputDF.schema.fields.map(_.name)
    assert(outputFields === Array("Number", "Col1", "Col2"))
  }
}

object FlattenColumnSpec {
  val Data = Seq(
    Row.fromTuple(1, ("val1", "val2")),
    Row.fromTuple(6, ("val2_1", "val2_2")),
    Row.fromTuple(3, ("val3_1", "val3_2"))
  )
  val Schema = StructType(Array(
    StructField("Number", IntegerType, true),
    StructField("SomeEmbeddedCol", new StructType(Array(
      StructField("Col1", DataTypes.StringType),
      StructField("Col2", DataTypes.StringType)
    )), true)
  )
  )

}