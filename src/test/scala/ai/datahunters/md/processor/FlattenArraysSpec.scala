package ai.datahunters.md.processor

import ai.datahunters.md.{SparkBaseSpec, UnitSpec}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{ArrayType, DataTypes, StructField, StructType}

class FlattenArraysSpec extends UnitSpec with SparkBaseSpec {

  import FlattenArraysSpec._

  "A FlattenArrays" should "convert array columns to string" in {
    val processor = FlattenArrays("|")(Seq("testCol1"))
    val rdd = spark.sparkContext.parallelize(SamplData)
    val df = spark.createDataFrame(rdd, Schema)
    val outputDF = processor.execute(df)
    val fields = outputDF.schema.fields
    assert(fields(0).name === "Id")
    assert(fields(0).dataType === DataTypes.StringType)
    assert(fields(1).name === "testCol1")
    assert(fields(1).dataType === DataTypes.StringType)
    val rows = outputDF.select("testCol1").collect()
    assert(rows(0).getString(0) === "person|car")
    assert(rows(1).getString(0) === "truck")
    assert(rows(2).getString(0) === "")
  }

}

object FlattenArraysSpec {

  val SamplData = Seq(
    Row.fromTuple("id1", Seq("person", "car")),
    Row.fromTuple("id2", Seq("truck")),
    Row.fromTuple("id3", Seq[String]())
  )

  val Schema = StructType(Array(
    StructField("Id", DataTypes.StringType, true),
    StructField("testCol1", DataTypes.createArrayType(DataTypes.StringType)))
  )
}