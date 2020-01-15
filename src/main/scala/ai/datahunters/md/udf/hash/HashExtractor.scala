package ai.datahunters.md.udf.hash

import ai.datahunters.md.util.HashUtils
import org.apache.spark.sql.functions.udf

object HashExtractor {

  def generateHashesUDF(hashMethodsList: Seq[String]) = {
    udf(generateHashesFromContent(hashMethodsList) _)
  }

  def generateHashUDF(hashMethod: String) = {
    udf(generateHashFromContent(hashMethod) _)
  }

  private def generateHashesFromContent(hashMethodsList: Seq[String])(content: Array[Byte]) = {
    hashMethodsList
      .map(hashType => Tuple2(
        hashType, HashUtils.HashExecutionMap
          .get(hashType)
          .map(f => f.apply(content))
      )).map(_._2.get)
  }

  private def generateHashFromContent(hashMethod: String)(content: Array[Byte]) = {
    HashUtils.HashExecutionMap.get(hashMethod).map(f => f.apply(content)).map(_._2)
  }
}
