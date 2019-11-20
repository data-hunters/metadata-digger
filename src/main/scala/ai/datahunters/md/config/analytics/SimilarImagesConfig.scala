package ai.datahunters.md.config.analytics

import ai.datahunters.md.config.ConfigLoader
import ai.datahunters.md.util.{Parser, ParserOpt}
import ai.datahunters.md.{MetatagSimilarityDefinition, TagValueType}
import com.typesafe.config.Config

/**
  * Configuration for rule based algorithm finding similar images to specified.
  *
  * @param tags Definitions of tags that have to be used to compare base image with others.
  * @param minPassingConditions Minimum number of conditions that have to be fulfilled to classify another image as similar.
  */
case class SimilarImagesConfig(tags: Seq[MetatagSimilarityDefinition],
                               minPassingConditions: Int)


object SimilarImagesConfig {

  import ai.datahunters.md.config.ConfigLoader.ListElementsDelimiter

  val DirTagDelimiter = "\\."
  val TagThresholdDelimiter = ":"
  val DefaultMaxDifference = 0

  val TagsInfoKey = "analytics.similar.tags"
  val MinPassingConditionsKey = "analytics.similar.minPassingConditions"

  val Defaults = Map(
    MinPassingConditionsKey -> -1
  )

  def build(config: Config): SimilarImagesConfig = {
    val configWithDefaults = ConfigLoader.assignDefaults(config, Defaults)
    val tags = parseTags(config.getString(TagsInfoKey))
    val minPassing = configWithDefaults.getInt(MinPassingConditionsKey)
    SimilarImagesConfig(
      tags,
      if (minPassing == -1) tags.size else minPassing
    )
  }

  private def parseTags(tags: String): Seq[MetatagSimilarityDefinition] = {
    tags.split(ListElementsDelimiter)
      .map(parseTag)
  }

  /**
    * Parse tag in the following format: "<DIR_NAME>:<TAG_NAME>:<TYPE>:<MAX_DIFFERENCE>" (where <MAX_DIFFERENCE> is optional) e.g.:
    * "File Type.Detected File Type Name:String:2" or "File Type.Detected File Type Name:String"
    *
    * @param tagInfo
    * @return
    */
  private def parseTag(tagInfo: String): MetatagSimilarityDefinition = {
    val tagInfoSplit = tagInfo.split(TagThresholdDelimiter)
    val fullTagNameArr = tagInfoSplit.head
    val fullTagName = fullTagNameArr.split(DirTagDelimiter)
    val dirName = fullTagName(0)
    val tagName = fullTagName(1)
    val tagValueType = TagValueType.withName(tagInfoSplit(1))
    val maxDifference = if (tagInfoSplit.size == 2) {
      BigDecimal(DefaultMaxDifference)
    } else if (tagInfoSplit.size == 3) {
      val maxDiffStr = tagInfoSplit(2)
      try {
        Parser.parse[BigDecimal](maxDiffStr)
      } catch {
        case e: Exception => throw new RuntimeException(s"Maximum Difference value - $maxDiffStr couldn't be parsed for tag: $tagInfo", e)
      }
    } else {
      throw new RuntimeException(s"Type of tag has not been specified properly in: $tagInfo (example: File Type.Detected File Type Name:String:2)")
    }
    MetatagSimilarityDefinition(dirName, tagName, tagValueType, maxDifference)
  }
}
