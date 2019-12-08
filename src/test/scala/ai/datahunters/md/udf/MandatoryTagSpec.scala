package ai.datahunters.md.udf

import ai.datahunters.md.{MandatoryTag, SparkBaseSpec, UnitSpec}
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

class MandatoryTagSpec extends UnitSpec with SparkBaseSpec {

  import MandatoryTagSpec._

  "notEmptyTagValueUDF" should " return one row" in {
    val mandatoryTag = MandatoryTag(Dir1Name, Tag2Name)
    val underTests = Filters.notEmptyTagValueUDF(mandatoryTag)
    val rdd = sparkSession.sparkContext.parallelize(Data)
    val df = sparkSession.createDataFrame(rdd, Schema)
    val result = df.filter(underTests(col(MetadataString))).count()
    assert(result === 1)
  }

  "notEmptyTagValueUDF" should " return all rows" in {
    val mandatoryTag = MandatoryTag(Dir2Name, Tag3Name)
    val underTests = Filters.notEmptyTagValueUDF(mandatoryTag)
    val rdd = sparkSession.sparkContext.parallelize(Data)
    val df = sparkSession.createDataFrame(rdd, Schema)
    val result = df.filter(underTests(col(MetadataString))).count()
    assert(result === 2)
  }

  "notEmptyTagValueUDF" should " return empty DF" in {
    val mandatoryTag = MandatoryTag(Dir3Name, Tag3Name)
    val underTests = Filters.notEmptyTagValueUDF(mandatoryTag)
    val rdd = sparkSession.sparkContext.parallelize(Data)
    val df = sparkSession.createDataFrame(rdd, Schema)
    val result = df.filter(underTests(col(MetadataString))).count()
    assert(result === 0)
  }
}

object MandatoryTagSpec {

  val MetadataString = "metadata"
  val Dir1Name = "dir1"
  val Dir2Name = "dir2"
  val Dir3Name = "dir3"
  val Tag1Name = "tag1"
  val Tag2Name = "tag2"
  val Tag3Name = "tag3"
  val Tag4Name = "tag4"
  val Value = "AnyValue"

  val Data = Seq(
    Row.fromSeq(Seq(Row.fromSeq(Seq(Map(Tag1Name -> Value), Map(Tag3Name -> Value, Tag4Name -> Value))))),
    Row.fromSeq(Seq(Row.fromSeq(Seq(Map(Tag2Name -> Value), Map(Tag3Name -> Value, Tag4Name -> Value)))))
  )

  val Schema = StructType(
    Array(
      StructField(MetadataString, DataTypes.createStructType(
        Array(
          StructField(Dir1Name, DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType)),
          StructField(Dir2Name, DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType))
        )
      ))
    )
  )

}
