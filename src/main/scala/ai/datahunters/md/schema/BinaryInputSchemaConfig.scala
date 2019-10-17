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
case class BinaryInputSchemaConfig() extends SchemaConfig {

  import BinaryInputSchemaConfig._

  override def columns(): Seq[String] = Seq(
    ContentHash,
    BasePathCol,
    FilePathCol,
    FileCol
  )

  override def schema(): StructType = new SchemaBuilder()
      .addStringField(ContentHash)
      .addStringField(BasePathCol)
      .addStringField(FilePathCol)
      .addBinaryField(FileCol)
      .build()

}

object BinaryInputSchemaConfig {

  val ContentHash = "ID"
  val BasePathCol = "Base Path"
  val FilePathCol = "File Path"
  val FileCol = "File Data"

}
