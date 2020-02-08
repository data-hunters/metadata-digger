package ai.datahunters.md.processor

import ai.datahunters.md.schema.BinaryInputSchemaConfig
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

/**
  * Hash extractor processor invokes processing of all provided(via config file) hash types.
  * @param hashTypes
  */
case class HashExtractor(hashTypes: Seq[String]) extends Processor {

  import ai.datahunters.md.processor.HashExtractor._
  import ai.datahunters.md.udf.hash.HashExtractor._

  override def execute(inputDF: DataFrame): DataFrame = {
    hashTypes.foldLeft(inputDF)(
      (previousDF, hash) =>
        previousDF.withColumn(HASH_COL_PREFIX + hash, generateHashUDF(hash)(col(BinaryInputSchemaConfig.FileCol)))
    )
  }
}

object HashExtractor {
  val HASH_COL_PREFIX = "hash_"
}


