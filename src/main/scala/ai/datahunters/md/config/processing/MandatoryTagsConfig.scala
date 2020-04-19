package ai.datahunters.md.config.processing

import ai.datahunters.md.MandatoryTag

/**
  * Configuration for mandatory tags filter. Include String to MandatoryTag model parser.
  *
  * @param dirTags Optional sequence of MandatoryTag class which includes dir and tag String fields.
  */
case class MandatoryTagsConfig(dirTags: Option[Seq[MandatoryTag]] = None)

object MandatoryTagsConfig {

  val DirTagDelimiter = "\\."


  def build(config: ProcessingConfig): MandatoryTagsConfig = {
    val tags = config.mandatoryTags
    MandatoryTagsConfig(getMandatoryTagSeq(tags))
  }

  def getMandatoryTagSeq(tags: Option[Seq[String]]): Option[Seq[MandatoryTag]] = {
    tags.map(s => s.filter(s=>s.nonEmpty).map(parseTag))
  }

  def parseTag(dirTag: String): MandatoryTag = {
    val dirTagArray = dirTag.split(DirTagDelimiter)
    MandatoryTag(dirTagArray(0), dirTagArray(1))
  }

}
