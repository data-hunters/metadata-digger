package ai.datahunters.md.config

import ai.datahunters.md.config.ConfigLoader.assignDefaults
import com.typesafe.config.Config

case class ProcessingConfig(val allowedDirectories: Option[Seq[String]],
                            val allowedTags: Option[Seq[String]],
                            val namingConvention: String,
                            val metadataColumnsPrefix: String,
                            val includeDirsInTags: Boolean) {

}

object ProcessingConfig {

  val AllowedDirectoriesKey = "filter.allowedMetadataDirectories"
  val AllowedTagsKey = "filter.allowedTags"
  val IncludeDirectoriesInTagNamesKey = "output.columns.includeDirsInTags"
  val MetadataColumnsPrefixKey = "output.columns.metadataPrefix"
  val ColumnsNamingConventionKey = "output.columns.namingConvention"

  import ConfigLoader.{All, strToSeq}


  val Defaults = Map(
    AllowedDirectoriesKey -> All,
    AllowedTagsKey -> All,
    ColumnsNamingConventionKey -> "camelCase",
    MetadataColumnsPrefixKey -> "",
    IncludeDirectoriesInTagNamesKey -> false
  )

  def build(config: Config): ProcessingConfig = {
    val configWithDefaults = assignDefaults(config, Defaults)
    ProcessingConfig(
      configWithDefaults.getString(AllowedDirectoriesKey),
      configWithDefaults.getString(AllowedTagsKey),
      configWithDefaults.getString(ColumnsNamingConventionKey),
      configWithDefaults.getString(MetadataColumnsPrefixKey),
      configWithDefaults.getBoolean(IncludeDirectoriesInTagNamesKey)
    )
  }


}