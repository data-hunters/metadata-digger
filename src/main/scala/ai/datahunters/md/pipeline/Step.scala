package ai.datahunters.md.pipeline

import org.apache.spark.sql.DataFrame

trait Step {

  def execute(inputDF: DataFrame): DataFrame
}
