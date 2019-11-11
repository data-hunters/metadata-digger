package ai.datahunters.md.processor

import ai.datahunters.md.util.TextUtils
import ai.datahunters.md.{SparkBaseSpec, UnitSpec}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

class ColumnNamesConverterSpec extends UnitSpec with SparkBaseSpec {

  import ColumnNamesConverterSpec._

  "A ColumnNamesConverter" should "convert all column names including nested to CamelCase" in {
    val rdd = sparkSession.sparkContext.parallelize(Data)
    val df = sparkSession.createDataFrame(rdd, Schema)
    val processor = ColumnNamesConverter(TextUtils.camelCase)
    val renamedDF = processor.execute(df)
    val names1 = renamedDF.schema.fields.map(_.name)
    val types1 = renamedDF.schema.fields.map(_.dataType)
    assert(names1 === Array("SomeNumber", "SomeNestedCol"))
    assert(types1 === Array(IntegerType, StructType(Array(
      StructField("NestedCol1", StringType, false),
      StructField("NestedCol2", StructType(Array(
        StructField("VeryNestedCol1", IntegerType),
        StructField("VeryNestedCol2", StringType)
      )))
    ))))
  }

  it should "convert all column names including nested to snake_case" in {
    val rdd = sparkSession.sparkContext.parallelize(Data)
    val df = sparkSession.createDataFrame(rdd, Schema)
    val processor = ColumnNamesConverter(TextUtils.snakeCase)
    val renamedDF = processor.execute(df)
    val names1 = renamedDF.schema.fields.map(_.name)
    val types1 = renamedDF.schema.fields.map(_.dataType)
    assert(names1 === Array("some_number", "some_nested_col"))
    assert(types1 === Array(IntegerType, StructType(Array(
      StructField("nested_col_1", StringType, false),
      StructField("nested_col2", StructType(Array(
        StructField("very_nested_col_1", IntegerType),
        StructField("very_nested_col_2", StringType)
      )))
    ))))
  }
}

object ColumnNamesConverterSpec {

  val Data = Seq(
    Row.fromTuple(1, Row.fromTuple("val1", Row.fromTuple(22, "nested val2"))),
    Row.fromTuple(6, Row.fromTuple("val2_1", Row.fromTuple(15, "nested val2_2"))),
    Row.fromTuple(3, Row.fromTuple("val3_1", Row.fromTuple(3, "nested val3_2")))
  )
  val Schema = StructType(Array(
    StructField("Some number", IntegerType, true),
    StructField("Some nested Col", new StructType(Array(
      StructField("Nested Col 1", DataTypes.StringType, nullable = false),
      StructField("Nested Col2", StructType(Array(
        StructField("Very Nested Col 1", DataTypes.IntegerType),
        StructField("Very Nested Col 2", DataTypes.StringType)
      )))
    )), true)
  )
  )

}