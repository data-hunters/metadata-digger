package ai.datahunters.md.udf.analytics

import ai.datahunters.md.{MetatagSimilarity, MetatagSimilarityDefinition, TagValueType}
import ai.datahunters.md.util.TextUtils
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

import scala.util.Try

object Similarities {


  /**
    * UDF Calculating Levenshtein distance and returning number of distances that are lower than specified threshold
    * @param tags List of tags that have to be compared to verify similarity
    * @return
    */
  def checkLevDistance(tags: Seq[MetatagSimilarity[String]]): UserDefinedFunction = udf(Transformations.checkLevDistanceT(tags) _)


  /**
    * UDF calculating how many tag values of Integer type are lower than specified threshold.
    *
    * @param tags List of tags that have to be compared to verify similarity
    * @return
    */
  def checkSimilarInts(tags: Seq[MetatagSimilarity[Int]]): UserDefinedFunction = udf(Transformations.checkSimilarIntsT(tags) _)


  /**
    * UDF calculating how many tag values of Long type are lower than specified threshold.
    *
    * @param tags List of tags that have to be compared to verify similarity
    * @return
    */
  def checkSimilarLongs(tags: Seq[MetatagSimilarity[Long]]): UserDefinedFunction = udf(Transformations.checkSimilarLongsT(tags) _)

  /**
    * UDF calculating how many tag values of Float type are lower than specified threshold.
    *
    * @param tags List of tags that have to be compared to verify similarity
    * @return
    */
  def checkSimilarFloats(tags: Seq[MetatagSimilarity[Float]]): UserDefinedFunction = udf(Transformations.checkSimilarFloatsT(tags) _)


  object Transformations {

    def checkLevDistanceT(tags: Seq[MetatagSimilarity[String]])(allTags: Map[String, Map[String, String]]): Int = {
      val numberOfPassingDistances = tags.map(t => {
        val maxThreshold = t.definition.maxDifference.toInt
        getTagValue(t, allTags)
          .exists(v => TextUtils.levenshteinDistance(t.value, v) <= maxThreshold)
      }).count(r => r)
      numberOfPassingDistances
    }

    /**
      * Checks all input Metatags by comparing its values with values of the same tags of particular row/file.
      *
      * @param tags List of tags for comparison with value of base file
      * @param allTags All tags of particular Row
      * @return
      */
    implicit def checkSimilarIntsT(tags: Seq[MetatagSimilarity[Int]])(allTags: Map[String, Map[String, String]]): Int  = {
      countSimilarTags(tags, allTags, (maxThreshold: BigDecimal, baseValue: Int, targetValue: Int) => {
        Math.abs(targetValue - baseValue) <= maxThreshold.toInt
      })
    }

    /**
      * Checks all input Metatags by comparing its values with values of the same tags of particular row/file.
      *
      * @param tags List of tags for comparison with value of base file
      * @param allTags All tags of particular Row
      * @return
      */
    implicit def checkSimilarLongsT(tags: Seq[MetatagSimilarity[Long]])(allTags: Map[String, Map[String, String]]): Int  = {
      countSimilarTags(tags, allTags, (maxThreshold: BigDecimal, baseValue: Long, targetValue: Long) => {
        Math.abs(targetValue - baseValue) <= maxThreshold.toLong
      })
    }

    /**
      * Checks all input Metatags by comparing its values with values of the same tags of particular row/file.
      *
      * @param tags
      * @param allTags
      * @return
      */
    implicit def checkSimilarFloatsT(tags: Seq[MetatagSimilarity[Float]])(allTags: Map[String, Map[String, String]]): Int  = {
      countSimilarTags(tags, allTags, (maxThreshold: BigDecimal, baseValue: Float, targetValue: Float) => {
        Math.abs(targetValue - baseValue) <= maxThreshold.toFloat
      })
    }

    private def countSimilarTags[T](tags: Seq[MetatagSimilarity[T]],
                                    allTags: Map[String, Map[String, String]],
                                    similarityAlgorithm: (BigDecimal, T, T) => Boolean) = {
      tags.map(t => {
        getTagValue(t, allTags)
          .exists(v => {
            val parsedTargetVal = Try(TagValueType.parse(t.definition.valueType, v).asInstanceOf[T]).toOption
            parsedTargetVal.map(v => similarityAlgorithm(t.definition.maxDifference, t.value, v))
              .getOrElse(false)
          })
      }).count(r => r)
    }

    private def getTagValue[T](baseFileMetatag: MetatagSimilarity[T], targetTags: Map[String, Map[String, String]]) = {
      targetTags.get(baseFileMetatag.definition.directory)
        .flatMap(dirTags => {
          dirTags.get(baseFileMetatag.definition.tag)
        })
    }
  }


}
