package ai.datahunters.md.writer

import org.apache.spark.sql.DataFrame

trait PipelineSink {

  def write(outputDF: DataFrame): Unit

}
