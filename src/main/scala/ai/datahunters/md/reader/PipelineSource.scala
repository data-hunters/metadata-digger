package ai.datahunters.md.reader

import org.apache.spark.sql.DataFrame

/**
  * Base interface for module loading data in whole pipeline.
  */
trait PipelineSource {

  def load(): DataFrame
}
