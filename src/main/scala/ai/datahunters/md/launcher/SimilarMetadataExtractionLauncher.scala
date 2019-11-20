package ai.datahunters.md.launcher

import ai.datahunters.md.MetatagSimilarity
import ai.datahunters.md.config.ConfigLoader
import ai.datahunters.md.config.analytics.SimilarImagesConfig
import ai.datahunters.md.filter.Filter
import ai.datahunters.md.filter.analytics.SimilarMetatagsFilter
import ai.datahunters.md.preprocessing.LocalMetadataExtractor
import com.typesafe.config.Config

/**
  * Launches job which finds similar images based on metadata.
  */
object SimilarMetadataExtractionLauncher {

  import BasicExtractorLauncher._

  def main(args: Array[String]): Unit = {
    val appInputArgs = AppArguments.parseComparisonArgs(args)
    val config: Config = ConfigLoader.load(appInputArgs.basicArgs.configPath)
    val filter = buildSimilarImgsFilter(config, appInputArgs)
    buildWorkflow(appInputArgs.basicArgs, config, Seq(filter)).run()
  }

  private def buildSimilarImgsFilter(config: Config, appInputArgs: ComparisonAppArguments): Filter = {
    val analyticsConfig = SimilarImagesConfig.build(config)
    val baseImgTags = loadBaseImage(appInputArgs.baseFilePath, analyticsConfig)
    SimilarMetatagsFilter(baseImgTags, analyticsConfig.minPassingConditions)
  }

  private def loadBaseImage(path: String, config: SimilarImagesConfig) = {
    val imgMetadata = LocalMetadataExtractor.extract(path)
    config.tags
        .map(t => {
          val dirName = t.directory
          val tagName = t.tag
          val tagValue = imgMetadata.tags.get(dirName)
            .flatMap(dir => {
              dir.get(tagName)
            })
          if (tagValue.isEmpty) throw new RuntimeException(s"Tag: $dirName.$tagName does not exist in base image!")
          MetatagSimilarity.create(t, tagValue.get).asInstanceOf[MetatagSimilarity[Any]]
        })
  }

}
