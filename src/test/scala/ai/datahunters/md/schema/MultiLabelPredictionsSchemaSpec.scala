package ai.datahunters.md.schema

import ai.datahunters.md.UnitSpec
import ai.datahunters.md.schema.BinaryInputSchemaConfig.{BasePathCol, FileCol, FilePathCol, IDCol}
import com.intel.analytics.bigdl.serialization.Bigdl.DataType
import org.apache.spark.sql.types.{ArrayType, BinaryType, DataTypes, StringType}

class MultiLabelPredictionsSchemaSpec extends UnitSpec {

  "A MultiLabelPredictionsSchema" should "keep info about schema for results of MultiLabel classifier" in {
    val schemaConfig = MultiLabelPredictionSchemaConfig()
    assert(schemaConfig.columns() === Seq(IDCol, BasePathCol, FilePathCol, FileCol, MultiLabelPredictionSchemaConfig().LabelsCol))
  }

  it should "provide types of fields for results of MultiLabel classifier" in {
    val schema = MultiLabelPredictionSchemaConfig().schema()
    val fields = schema.fields
    assert(fields.size === 5)
    assert(fields(0).name === IDCol)
    assert(fields(0).dataType === StringType)
    assert(fields(1).name === BasePathCol)
    assert(fields(1).dataType === StringType)
    assert(fields(2).name === FilePathCol)
    assert(fields(2).dataType === StringType)
    assert(fields(3).name === FileCol)
    assert(fields(3).dataType === BinaryType)
    assert(fields(4).name === MultiLabelPredictionSchemaConfig().LabelsCol)
    assert(fields(4).dataType === DataTypes.createArrayType(StringType))
  }

}
