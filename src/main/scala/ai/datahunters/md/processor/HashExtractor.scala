package ai.datahunters.md.processor

import ai.datahunters.md.schema.BinaryInputSchemaConfig
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

case class HashExtractor(hashTypes: Option[Seq[String]] = None) extends Processor {

  import ai.datahunters.md.udf.hash.HashExtractor._

  override def execute(inputDF: DataFrame): DataFrame = {
    hashTypes.filter(_.nonEmpty)
      .getOrElse(Seq())
      .foldLeft(inputDF)(
        (previousDF, hash) => previousDF.withColumn(hash, generateHashUDF(hash)(col(BinaryInputSchemaConfig.FileCol)))
      )
  }
}
