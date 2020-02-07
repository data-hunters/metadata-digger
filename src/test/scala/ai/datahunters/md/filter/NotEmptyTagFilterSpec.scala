package ai.datahunters.md.filter

import ai.datahunters.md.{MandatoryTag, SparkBaseSpec, UnitSpec}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

class NotEmptyTagFilterSpec extends UnitSpec with SparkBaseSpec {

  import NotEmptyTagFilterSpec._

  "a NotEmptyTagFilter" should "pass all rows due to lack of mandatory tag" in {
    val filter = new NotEmptyTagFilter(Seq[MandatoryTag]())
    val rdd = sparkSession.sparkContext.parallelize(Data)
    val df = sparkSession.createDataFrame(rdd, Schema)
    val outputDF = filter.execute(df)
    val collectedOutput = outputDF.collect()
    assert(collectedOutput.length === 2)
  }

  "a NotEmptyTagFilter" should "exclude not passing row" in {
    val filter = new NotEmptyTagFilter(Seq[MandatoryTag](MandatoryTag(Dir1Name, Tag1Name)))
    val rdd = sparkSession.sparkContext.parallelize(Data)
    val df = sparkSession.createDataFrame(rdd, Schema)
    val outputDF = filter.execute(df)
    val collectedOutput = outputDF.collect()
    assert(collectedOutput.length === 1)
  }

  "a NotEmptyTagFilter" should "pass all row" in {
    val filter = new NotEmptyTagFilter(Seq[MandatoryTag](MandatoryTag(Dir2Name, Tag3Name)))
    val rdd = sparkSession.sparkContext.parallelize(Data)
    val df = sparkSession.createDataFrame(rdd, Schema)
    val outputDF = filter.execute(df)
    val collectedOutput = outputDF.collect()
    assert(collectedOutput.length === 2)
  }

  "a NotEmptyTagFilter" should "not pass any row" in {
    val filter = new NotEmptyTagFilter(Seq[MandatoryTag](MandatoryTag(Dir3Name, Tag3Name)))
    val rdd = sparkSession.sparkContext.parallelize(Data)
    val df = sparkSession.createDataFrame(rdd, Schema)
    val outputDF = filter.execute(df)
    val collectedOutput = outputDF.collect()
    assert(collectedOutput.length === 0)
  }
}

object NotEmptyTagFilterSpec {

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