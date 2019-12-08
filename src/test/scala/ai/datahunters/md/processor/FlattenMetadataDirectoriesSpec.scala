package ai.datahunters.md.processor

import ai.datahunters.md.schema.{EmbeddedMetadataSchemaConfig, MetadataSchemaConfig, MetadataTagsSchemaConfig}
import ai.datahunters.md.{SparkBaseSpec, UnitSpec}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataTypes, IntegerType, StructField, StructType}

class FlattenMetadataDirectoriesSpec extends UnitSpec with SparkBaseSpec {
  import FlattenMetadataDirectoriesSpec._

  "A FlattenMetadataDirectories" should "extract embedded directories to root level" in {
    val processor = FlattenMetadataDirectories()
    val rdd = sparkSession.sparkContext.parallelize(Data)
    val df = sparkSession.createDataFrame(rdd, Schema).repartition(1)
    val outputDF = processor.execute(df)
    val outputFields = outputDF.schema.fields.map(_.name)
    assert( Array("Path", MetadataTagsSchemaConfig.MetadataCol, EmbeddedMetadataSchemaConfig.DirectoryNames, EmbeddedMetadataSchemaConfig.TagNamesCol) === outputFields)
    val row1 = outputDF.collect()(0)
    val mdField = row1.getStruct(row1.fieldIndex(MetadataTagsSchemaConfig.MetadataCol))
    val mdFieldNames = mdField.schema.fields.map(_.name)
    assert(Array("dir1", "dir2") === mdFieldNames)
  }

  it should "extract selected embedded directories to root level" in {
    val processor = FlattenMetadataDirectories(Some(Seq("dir2")))
    val rdd = sparkSession.sparkContext.parallelize(Data)
    val df = sparkSession.createDataFrame(rdd, Schema).repartition(1)
    val outputDF = processor.execute(df)
    val outputFields = outputDF.schema.fields.map(_.name)
    val row1 = outputDF.collect()(0)
    val mdField = row1.getStruct(row1.fieldIndex(MetadataTagsSchemaConfig.MetadataCol))
    val mdFieldNames = mdField.schema.fields.map(_.name)
    assert(mdFieldNames === Array("dir2"))
  }
}

object FlattenMetadataDirectoriesSpec {

  val Data = Seq(
    Row.fromTuple("/some/path1", Row.fromTuple(3, Map("dir1" -> Map("tag1" -> "val1"), "dir2" -> Map("tag3" -> "val3", "tag4" -> "val4")), Seq("dir1", "dir2"), Seq("tag1", "tag3", "tag4"))),
    Row.fromTuple("/some/path2", Row.fromTuple(3, Map("dir1" -> Map("tag1" -> "val1", "tag2" -> "val2"), "dir2" -> Map("tag5" -> "val5")), Seq("dir1", "dir2"), Seq("tag1", "tag2", "tag5")))
  )

  val Schema = StructType(
    Array(
      StructField("Path", DataTypes.StringType),
      StructField(MetadataTagsSchemaConfig.MetadataCol, StructType(
        Array(
          StructField(EmbeddedMetadataSchemaConfig.TagsCountCol, DataTypes.IntegerType),
          StructField(EmbeddedMetadataSchemaConfig.TagsCol, DataTypes.createMapType(DataTypes.StringType, DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType))),
          StructField(EmbeddedMetadataSchemaConfig.DirectoryNames, DataTypes.createArrayType(DataTypes.StringType)),
          StructField(EmbeddedMetadataSchemaConfig.TagNamesCol, DataTypes.createArrayType(DataTypes.StringType))
        )
      )
      )
    )
  )

}