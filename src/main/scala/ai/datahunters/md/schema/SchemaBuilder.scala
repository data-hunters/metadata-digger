package ai.datahunters.md.schema

import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

import scala.collection.mutable.ArrayBuffer

/**
  * Provides mechanism for easier DataFrame Schema creation.
  */
class SchemaBuilder {

  private val fields = ArrayBuffer[StructField]()

  def build(): StructType = StructType(fields)

  def addStringField(name: String): SchemaBuilder = {
    fields.append(StructField(name, DataTypes.StringType))
    this
  }

  def addIntField(name: String): SchemaBuilder = {
    fields.append(StructField(name, DataTypes.IntegerType))
    this
  }

  def addStringArrayField(name: String): SchemaBuilder = {
    fields.append(StructField(name, DataTypes.createArrayType(DataTypes.StringType)))
    this
  }

  def addStringMap(name: String): SchemaBuilder = {
    fields.append(
      StructField(name, DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType))
    )
    this
  }

  def addEmbeddedMap(name: String): SchemaBuilder = {
    fields.append(
      StructField(name, DataTypes.createMapType(DataTypes.StringType, DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType)))
    )
    this
  }

  def addStringFields(names: Seq[String]): SchemaBuilder = {
    names.foreach(addStringField)
    this
  }

  def addStringMaps(names: Seq[String]): SchemaBuilder = {
    names.foreach(addStringMap)
    this
  }

  def addBinaryField(name: String): SchemaBuilder = {
    fields.append(
      StructField(name, DataTypes.BinaryType)
    )
    this
  }
}
