package ai.datahunters.md.udf.hash

import ai.datahunters.md.util.HashUtils
import org.apache.spark.sql.functions.udf

object HashExtractor {

  def generateHashUDF(hashMethod: String) = {
    udf(generateHashFromContent(hashMethod) _)
  }

  def generateHashFromContent(hashMethod: String)(content: Array[Byte]) = {
    HashUtils.HashExecutionMap.
      get(hashMethod.toLowerCase())
      .map(f => f(content))
      .map(_._2)
  }
}
