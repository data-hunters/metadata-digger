package ai.datahunters.md.config.processing

import ai.datahunters.md.config.{ConfigLoader, Writer}
import com.typesafe.config.Config

case class ProcessingConfig(allowedDirectories: Option[Seq[String]],
                            allowedTags: Option[Seq[String]],
                            mandatoryTags: Option[Seq[String]],
                            namingConvention: String,
                            metadataColumnsPrefix: String,
                            includeDirsInTags: Boolean,
                            outputFormat: String)

object ProcessingConfig {

  val AllowedDirectoriesKey = "filter.allowedMetadataDirectories"
  val AllowedTagsKey = "filter.allowedTags"
  val IncludeDirectoriesInTagNamesKey = "output.columns.includeDirsInTags"
  val MetadataColumnsPrefixKey = "output.columns.metadataPrefix"
  val ColumnsNamingConventionKey = "output.columns.namingConvention"
  val MandatoryTagsKey = "filter.mandatoryTags"

  import ConfigLoader._


  val Defaults = Map(
    AllowedDirectoriesKey -> All,
    AllowedTagsKey -> All,
    ColumnsNamingConventionKey -> "camelCase",
    MetadataColumnsPrefixKey -> "",
    IncludeDirectoriesInTagNamesKey -> true,
    MandatoryTagsKey -> All
  )

  def build(config: Config): ProcessingConfig = {
    val configWithDefaults = assignDefaults(config, Defaults)
    ProcessingConfig(
      configWithDefaults.getString(AllowedDirectoriesKey),
      configWithDefaults.getString(AllowedTagsKey),
      configWithDefaults.getString(MandatoryTagsKey),
      configWithDefaults.getString(ColumnsNamingConventionKey),
      configWithDefaults.getString(MetadataColumnsPrefixKey),
      configWithDefaults.getBoolean(IncludeDirectoriesInTagNamesKey),
      configWithDefaults.getString(Writer.OutputFormatKey)
    )
  }


}