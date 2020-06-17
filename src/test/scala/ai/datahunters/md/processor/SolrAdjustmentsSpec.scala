package ai.datahunters.md.processor

import ai.datahunters.md.processor.FlattenMetadataTagsSpec.{Data, Schema}
import ai.datahunters.md.schema.{BinaryInputSchemaConfig, MetadataTagsSchemaConfig}
import ai.datahunters.md.{SparkBaseSpec, UnitSpec}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

class SolrAdjustmentsSpec extends UnitSpec with SparkBaseSpec {

  "A SolrAdjustments" should "flatten metadata and create column with content" in {
    val solrAdjustmentsProc = SolrAdjustments("MD ", true, true)
    val rdd = sparkSession.sparkContext.parallelize(Data)
    val df = sparkSession.createDataFrame(rdd, Schema)
    val outputDF = solrAdjustmentsProc.execute(df)
    val fields = outputDF.schema.fields
    val names = fields.map(_.name)
    assert(names === Array("Number", BinaryInputSchemaConfig.FilePathCol, MetadataTagsSchemaConfig.MetadataCol, "Metadata Content"))
    assert(fields.map(_.dataType) === Array(IntegerType,
      StringType,
      DataTypes.createMapType(StringType, StringType),
      StringType))
    val r1 = outputDF.collect().filter(_.getInt(0) == 1)(0)
    val mapCol1: Map[String, String] = r1.getAs(MetadataTagsSchemaConfig.MetadataCol)
    assert(mapCol1.keySet === Seq("MD Dir1 tag1", "MD Dir2 tag2").toSet)
  }

  it should "flatten metadata and create column without content and directory name" in {
    val solrAdjustmentsProc = SolrAdjustments("MD ", false, false)
    val rdd = sparkSession.sparkContext.parallelize(Data)
    val df = sparkSession.createDataFrame(rdd, Schema)
    val outputDF = solrAdjustmentsProc.execute(df)
    val fields = outputDF.schema.fields
    val names = fields.map(_.name)
    assert(names === Array("Number", BinaryInputSchemaConfig.FilePathCol, MetadataTagsSchemaConfig.MetadataCol))
    assert(fields.map(_.dataType) === Array(IntegerType,
      StringType,
      DataTypes.createMapType(StringType, StringType)))
    val r1 = outputDF.collect().filter(_.getInt(0) == 1)(0)
    val mapCol1: Map[String, String] = r1.getAs(MetadataTagsSchemaConfig.MetadataCol)
    assert(mapCol1.keySet === Seq("MD tag1", "MD tag2").toSet)
  }

}
