package ai.datahunters.md.schema

import ai.datahunters.md.{SparkBaseSpec, UnitSpec}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

class SchemaConfigSpec extends UnitSpec with SparkBaseSpec {

  import SchemaConfigSpec._

  "SchemaConfig" should "select particular columns except" in {
    val actualColumns = SchemaConfig.existingColumns(SampleSchema, Seq("col1"))
    assert(Seq("col2", "col3") === actualColumns)
  }

  it should "select particular columns from DF except" in {
    val rdd = sparkSession.sparkContext.parallelize(Seq[Row]())
    val df = sparkSession.createDataFrame(rdd, SampleSchema)
    val actualColumns = SchemaConfig.dfExistingColumns(df, Seq("col1"))

    assert(Seq("col2", "col3") === actualColumns)
  }

  it should "find field from schema" in {
    val rdd = sparkSession.sparkContext.parallelize(SampleData)
    val df = sparkSession.createDataFrame(rdd, SampleSchema)
    val field = SchemaConfig.findField(df, "col2")
    assert(field.name === "col2")
    assert(field.dataType.isInstanceOf[StringType])
  }

}

object SchemaConfigSpec {

  val SampleSchema = StructType(Array(
    StructField("col1", IntegerType),
    StructField("col2", StringType),
    StructField("col3", IntegerType)
  ))

  val SampleData = Seq(
    Row.fromTuple(1, "val1", 4),
    Row.fromTuple(6,  "val2", 1)
  )

}