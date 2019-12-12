package ai.datahunters.md.udf.hash

import ai.datahunters.md.util.HashUtils
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.StructType

object HashExtractor {

  def generateHashes(hashMethodsList: Seq[String]) = {
    udf(generateHashFromContent(hashMethodsList) _, new StructType())
  }

  private def generateHashFromContent(hashMethodsList: Seq[String])(content: Array[Byte]) = {
    hashMethodsList.map(a => HashUtils.HashExecutionMap.get(a).map(f => f.apply(content)))
  }

}
