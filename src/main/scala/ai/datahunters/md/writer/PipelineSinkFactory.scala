package ai.datahunters.md.writer

import ai.datahunters.md.config.writer.{FilesWriterConfig, LocalFSWriterConfig, SolrWriterConfig, WriterConfig}
import ai.datahunters.md.listener.OutputFilesCleanupListener
import ai.datahunters.md.util.SolrClientBuilder
import ai.datahunters.md.writer.solr.SolrForeachWriter
import org.apache.spark.sql.SparkSession

object PipelineSinkFactory {

  def create(config: WriterConfig, sparkSession: SparkSession): PipelineSink = config match {
    case fwc: FilesWriterConfig => buildBasicFileOutputWriter(sparkSession, fwc)
    case sc: SolrWriterConfig => buildSolrWriter(sparkSession, sc)
    case _ => throw new RuntimeException(s"Not supported Reader for ${config.getClass}")
  }


  protected def buildBasicFileOutputWriter(sparkSession: SparkSession, config: FilesWriterConfig): BasicFileOutputWriter = {
    if (sparkSession.sparkContext.isLocal && config.isInstanceOf[LocalFSWriterConfig]) {
      sparkSession.sparkContext.addSparkListener(new OutputFilesCleanupListener(sparkSession, config.outputDirPath, config.format))
    }
    BasicFileOutputWriter(sparkSession, config)
  }

  protected def buildSolrWriter(sparkSession: SparkSession, config: SolrWriterConfig): SolrWriter = {
    val client = SolrClientBuilder().setDefaultCollection(config.collection)
      .setZKSolrChroot(config.zkSolrZNode)
      .setZKServers(config.zkServers)
      .build()
    val foreachWriter = SolrForeachWriter(config)
    SolrWriter(foreachWriter, client)
  }
}
