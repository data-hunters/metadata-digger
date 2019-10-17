package ai.datahunters.md.reader

import ai.datahunters.md.config.{FilesReaderConfig, ReaderConfig}
import org.apache.spark.sql.SparkSession

/**
  * Build specific reader used in Processing Pipeline.
  */
object PipelineSourceFactory {

  def create(config: ReaderConfig, sparkSession: SparkSession): PipelineSource = config match {
    case c: FilesReaderConfig => BasicBinaryFilesReader(sparkSession, c.partitionsNum, c.inputPaths)
    case _ => throw new RuntimeException(s"Not supported Reader for ${config.getClass}")
  }
}
