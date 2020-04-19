package ai.datahunters.md.schema
import ai.datahunters.md.schema.BinaryInputSchemaConfig.{BasePathCol, FileCol, FilePathCol, IDCol}
import org.apache.spark.sql.types.StructType

case class MultiLabelPredictionSchemaConfig(hashList: Seq[String] = Seq()) extends SchemaConfig {

  val LabelsCol = "Labels"
  /**
    * List of all columns contained in schema
    *
    * @return
    */
  override def columns(): Seq[String] = Seq(
    BinaryInputSchemaConfig.IDCol,
    BinaryInputSchemaConfig.BasePathCol,
    BinaryInputSchemaConfig.FilePathCol,
    BinaryInputSchemaConfig.FileCol,
    LabelsCol
  )

  /**
    * Final schema
    *
    * @return
    */
  override def schema(): StructType = new SchemaBuilder()
    .addStringField(IDCol)
    .addStringField(BasePathCol)
    .addStringField(FilePathCol)
    .addBinaryField(FileCol)
    .addStringArrayField(LabelsCol)
    .addStringFields(hashList)
    .build()

}
