package ai.datahunters.md.processor
import ai.datahunters.md.util.TextUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{DataType, MapType, StructField, StructType}

/**
  * Convert names of all columns according to specific naming convention.
  *
  * @param namingConvention Function converting name from original form to the target
  */
case class ColumnNamesConverter(namingConvention: (String) => String) extends Processor {

  import org.apache.spark.sql.functions._

  protected def nc(name: String): String = TextUtils.safeName(namingConvention(name))

  override def execute(inputDF: DataFrame): DataFrame = {
    val fields = inputDF.schema.fields
    val newCols = fields.map(f => {
      if (f.dataType.isInstanceOf[StructType]) {
        col(f.name).cast(processType(f)).as(nc(f.name))
      } else {
        col(f.name).as(nc(f.name))
      }
    })
    inputDF.select(newCols:_*)
  }

  private def processField(structField: StructField): StructField = {
    if (structField.dataType.isInstanceOf[StructType]) {
      StructField(nc(structField.name), processType(structField), structField.nullable)
    } else {
      StructField(nc(structField.name), structField.dataType, structField.nullable)
    }
  }

  private def processType(field: StructField): DataType = {
    field.dataType match {
      case structType: StructType =>
        val r = StructType(structType.fields.map(
          f => processField(f)))
        r
      case baseType => baseType
    }
  }


}
