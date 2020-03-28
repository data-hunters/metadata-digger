package ai.datahunters.md.processor

import ai.datahunters.md.{SparkBaseSpec, UnitSpec}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

class HashComparatorSpec extends UnitSpec with SparkBaseSpec {

  import HashComparatorSpec._

  "a HashComparator" should "compare two data frames" in {
    val hashRdd = sparkSession.sparkContext.parallelize(HashData)
    val hashDF = Option(sparkSession.createDataFrame(hashRdd, HashSchema))
    val processor = HashComparator(hashDF, Option(Seq("crc32", "md5")))

    val inputRdd = sparkSession.sparkContext.parallelize(InputData)
    val inputDF = sparkSession.createDataFrame(inputRdd, InputSchema)

    val resultDF = processor.execute(inputDF)
    val collectedOutput = resultDF.collect()
    val result = collectedOutput.map(_.getString(2)).toSeq
    assert(collectedOutput.length === 3)
    assert(result.diff(Seq("id2", "id3", "id4")).size === 0)
  }

}

object HashComparatorSpec {

  val HashData = Seq(
    Row.fromSeq(Seq(
      "crc32 hash", "md5 hash"
    )))

  val HashSchema = StructType(
    Array(
      StructField("hash_crc32", DataTypes.StringType),
      StructField("hash_md5", DataTypes.StringType)
    )
  )

  val InputData = Seq(
    Row.fromSeq(Seq("id1", "crc32 hash", "md5 hash", "Existing example")),
    Row.fromSeq(Seq("id2", "nw crc32 hash", "md5 hash", "Correct example 1")),
    Row.fromSeq(Seq("id3", "crc32 hash", "new md5 hash", "Correct example 2")),
    Row.fromSeq(Seq("id4", "new crc32 hash", "new md5 hash", "Correct example 3"))
  )

  val InputSchema = StructType(
    Array(
      StructField("id", DataTypes.StringType),
      StructField("hash_crc32", DataTypes.StringType),
      StructField("hash_md5", DataTypes.StringType),
      StructField("custom_value", DataTypes.StringType)
    )
  )
}

