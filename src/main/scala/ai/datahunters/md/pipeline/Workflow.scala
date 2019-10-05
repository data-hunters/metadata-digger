package ai.datahunters.md.pipeline

/**
  * Contains and launches complete ProcessingPipeline with Reader and Writer part.
  */
trait Workflow {

  def run(): Unit
}
