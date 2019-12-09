package ai.datahunters.md.filter.analytics

import ai.datahunters.md.schema.{EmbeddedMetadataSchemaConfig, MetadataTagsSchemaConfig}
import ai.datahunters.md._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

class SimilarMetatagsFilterSpec extends UnitSpec with SparkBaseSpec {

  import SimilarMetatagsFilterSpec._

  "A SimilarMetatagsFilter" should "filter out one not similar file" in {
    val similarities1 = MetatagSimilarity.create(MetatagSimilarityDefinition("dir2", "tag4", TagValueType.FLOAT, 3), "7").asInstanceOf[MetatagSimilarity[Any]]
    val similarities2 = MetatagSimilarity.create(MetatagSimilarityDefinition("dir1", "tag1", TagValueType.STRING, 1), "val1").asInstanceOf[MetatagSimilarity[Any]]
    val filter = SimilarMetatagsFilter(Seq(similarities1, similarities2), 2)
    val rdd = sparkSession.sparkContext.parallelize(Data)
    val df = sparkSession.createDataFrame(rdd, Schema)
    val outputDF = filter.execute(df)
    val collectedOutput = outputDF.collect()
    assert(collectedOutput.size === 1)
    val r1 = collectedOutput(0)
    assert(r1.getAs[String]("Path") === "/some/path1")
  }
}

object SimilarMetatagsFilterSpec {

  val Data = Seq(
    Row.fromTuple("/some/path1", Row.fromTuple(3, Map("dir1" -> Map("tag1" -> "val1"), "dir2" -> Map("tag3" -> "30", "tag4" -> "4.1")), Seq("dir1", "dir2"))),
    Row.fromTuple("/some/path2", Row.fromTuple(3, Map("dir1" -> Map("tag1" -> "al1", "tag2" -> "val2"), "dir2" -> Map("tag3" -> "-40.1")), Seq("dir1", "dir2")))
  )

  val Schema = StructType(
    Array(
      StructField("Path", DataTypes.StringType),
      StructField(MetadataTagsSchemaConfig.MetadataCol, StructType(
        Array(
          StructField(EmbeddedMetadataSchemaConfig.TagsCountCol, DataTypes.IntegerType),
          StructField(EmbeddedMetadataSchemaConfig.TagsCol, DataTypes.createMapType(DataTypes.StringType, DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType))),
          StructField(EmbeddedMetadataSchemaConfig.DirectoryNamesCol, DataTypes.createArrayType(DataTypes.StringType))
        )
      )
      )
    )
  )
}