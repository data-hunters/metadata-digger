package ai.datahunters.md.udf

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

object Converters {
  import Transformations._

  /**
    * UDF converting keys of Map to specific naming convention.
    * Input argument for UDF: Map[String, String]
    * UDF output: Map[String, String]
    * @param converter
    * @return
    */
  def convertMapKeys(converter: String => String): UserDefinedFunction = {
    udf(convertMapKeysT(converter) _)
  }

  object Transformations {

    def convertMapKeysT(converter: String => String)(map: Map[String, String]): Map[String, String] = {
      map.map(el => converter(el._1) -> el._2)
    }
  }
}
