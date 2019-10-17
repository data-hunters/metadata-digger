package ai.datahunters.md.schema

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{StructField, StructType}

/**
  * Provides base class for schemas used in particular steps of processing.
  */
trait SchemaConfig {

  /**
    * List of all columns contained in schema
    * @return
    */
  def columns(): Seq[String]

  /**
    * Final schema
    * @return
    */
  def schema(): StructType

}

object SchemaConfig {

  /**
    * Lists all existing columns in DataFrame with optional exclusion of some of them.
    * @param df
    * @param except
    * @return
    */
  def dfExistingColumns(df: DataFrame, except: Seq[String] = Seq()): Seq[String] = existingColumns(df.schema, except)

  /**
    * Lists all existing columns in Row Schema with optional exclusion of some of them.
    * @param df
    * @param except
    * @return
    */
  def rowExistingColumns(row: Row, except: Seq[String] = Seq()): Seq[String] = existingColumns(row.schema, except)

  /**
    * Lists all existing columns in Schema with optional exclusion of some of them.
    * @param schema
    * @param except
    * @return
    */
  def existingColumns(schema: StructType, except: Seq[String] = Seq()): Seq[String] = schema.fields
    .map(_.name)
    .filterNot(except.contains)

  /**
    * Find StructField object in passed DataFrame schema based on name.
    *
    * @param df
    * @param name
    * @return
    */
  def findField(df: DataFrame, name: String): StructField = {
    df.schema
      .fields
      .filter(f => name.equalsIgnoreCase(f.name))
      .head
  }
}