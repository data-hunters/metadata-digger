package ai.datahunters.md.processor

import ai.datahunters.md.{SparkBaseSpec, UnitSpec}
import ai.datahunters.md.schema.BinaryInputSchemaConfig
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}


class HashExtractorSpec extends UnitSpec with SparkBaseSpec {

  import HashExtractorSpec._

  "a HashExtractor" should "generate crc32 value" in {
    val processor = HashExtractor(Seq("crc32", "md5"))
    val rdd = sparkSession.sparkContext.parallelize(Data)
    val df = sparkSession.createDataFrame(rdd, InputSchema)
    val resultDF = processor.execute(df)
    val collectedOutput = resultDF.collect()
    val result = collectedOutput.map(_.getString(1))
    assert(collectedOutput.length === 1)
    assert(result === ExpectedCrc32Result)
  }

  "a HashExtractor" should "not generate crc32 value" in {
    val processor = HashExtractor(Seq())
    val rdd = sparkSession.sparkContext.parallelize(Data)
    val df = sparkSession.createDataFrame(rdd, InputSchema)
    val resultDF = processor.execute(df)
    val collectedOutput = resultDF.collect()
    val result = collectedOutput.map(_.schema)
    assert(collectedOutput.length === 1)
    assert(result.toSeq.size === 1)
  }
}

object HashExtractorSpec {

  val ExpectedCrc32Result = Array("884863d2")

  val Data = Seq(Row.fromSeq(Seq(new String("123").getBytes)))

  val InputSchema = StructType(
    Array(
      StructField(BinaryInputSchemaConfig.FileCol, DataTypes.BinaryType)
    )

  )
}
