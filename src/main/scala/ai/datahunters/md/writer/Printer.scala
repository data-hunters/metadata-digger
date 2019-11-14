package ai.datahunters.md.writer
import org.apache.spark.sql.DataFrame

/**
  * Below Sink should be used only for printing DataFrames containing summaries, to avoid OOM issues.
  */
object Printer extends PipelineSink {
  override def write(outputDF: DataFrame): Unit = {
    outputDF.show(Int.MaxValue, false)
  }
}
