package ai.datahunters.md.workflow

/**
  * Contains and launches complete ProcessingPipeline with Reader and Writer part.
  */
trait Workflow {

  def run(): Unit
}
