package ai.datahunters.md.config.processing

case class AllowedExtensionsConfig(extensions: Option[Seq[String]] = None)

object AllowedExtensionsConfig {

  def build(config: ProcessingConfig): AllowedExtensionsConfig = {
    AllowedExtensionsConfig(config.allowedExtensions)
  }

}
