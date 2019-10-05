package ai.datahunters.md.schema
import org.apache.spark.sql.types.StructType

/**
  * Provide DataFrame schema for input binary data like images, videos, etc.
  *
  * @param basePathCol Name of column with path to directory
  * @param pathIdCol Id of the path, can be used instead of base directory path for instance in further analysis requiring aggregation operations
  * @param filePathCol Path to particular path
  * @param fileCol Binary content of loaded file
  */
case class BinaryInputSchemaConfig(basePathCol: String = BinaryInputSchemaConfig.BasePathCol,
                                   pathIdCol: String = BinaryInputSchemaConfig.PathIdCol,
                                   filePathCol: String = BinaryInputSchemaConfig.FilePathCol,
                                   fileCol: String = BinaryInputSchemaConfig.FileCol) extends SchemaConfig {



  override def columns(): Seq[String] = Seq(
    basePathCol,
    pathIdCol,
    filePathCol,
    fileCol
  )

  override def schema(): StructType = new SchemaBuilder()
    .addStringField(basePathCol)
    .addStringField(pathIdCol)
    .addStringField(filePathCol)
    .addBinaryField(fileCol)
    .build()


}

object BinaryInputSchemaConfig {

  val BasePathCol = "BasePath"
  val PathIdCol = "PathID"
  val FilePathCol = "FilePath"
  val FileCol = "FileData"

}
