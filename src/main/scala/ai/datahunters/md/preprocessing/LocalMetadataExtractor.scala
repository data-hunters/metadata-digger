package ai.datahunters.md.preprocessing

import java.nio.file.{Files, Paths}

import ai.datahunters.md.{MetadataInfo, MetatagSimilarity}
import ai.datahunters.md.udf.MetadataExtractor

/**
  * Extracts metadata from Local File System.
  */
object LocalMetadataExtractor {

  def extract(path: String): MetadataInfo = {
    val file = Files.readAllBytes(Paths.get(path))
    MetadataExtractor.extract(file)
  }

}
