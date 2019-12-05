package ai.datahunters.md.config.processing

import ai.datahunters.md.MandatoryTag

/**
  * Configuration for mandatory tags filter. Include String to MandatoryTag model parser.
  * @param dirTags Optional sequence of MandatoryTag class which includes dir and tag String fields.
  */
case class MandatoryTagsConfig(dirTags: Option[Seq[MandatoryTag]])

object MandatoryTagsConfig {

  val DirTagDelimiter = "\\."
  var MandatoryTagDelimiter = ","


  def build(config: ProcessingConfig): MandatoryTagsConfig = {
    val tags = config.mandatoryTags
    if (tags.isEmpty) {
      MandatoryTagsConfig(None)
    } else {
      val parsedTags = parseMandatoryTag(tags)
      MandatoryTagsConfig(Option(parsedTags))
    }
  }

  def parseMandatoryTag(tags: String): Seq[MandatoryTag] = {
    tags.split(MandatoryTagDelimiter)
      .map(parseTag)
      .toSeq
  }

  def parseTag(dirTag: String): MandatoryTag = {
    val tagInfo = dirTag.split(DirTagDelimiter)
    val dir = tagInfo(0)
    val tag = tagInfo(1)
    MandatoryTag(dir, tag)
  }

}
