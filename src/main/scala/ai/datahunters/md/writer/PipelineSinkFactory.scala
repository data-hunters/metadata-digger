package ai.datahunters.md.writer

import java.nio.file.{Files, Paths}

import ai.datahunters.md.config.writer.{FilesWriterConfig, LocalFSWriterConfig, SolrWriterConfig, WriterConfig}
import ai.datahunters.md.listener.OutputFilesCleanupListener
import ai.datahunters.md.writer.solr.{SolrClientBuilder, SolrForeachWriter}
import org.apache.spark.SparkFiles
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

object PipelineSinkFactory {

  private val Logger = LoggerFactory.getLogger(PipelineSinkFactory.getClass)

  def create(config: WriterConfig, sparkSession: SparkSession): PipelineSink = config match {
    case fwc: FilesWriterConfig => buildBasicFileOutputWriter(sparkSession, fwc)
    case sc: SolrWriterConfig => buildSolrWriter(sparkSession, sc)
    case _ => throw new RuntimeException(s"Not supported Writer for ${config.getClass}")
  }


  protected def buildBasicFileOutputWriter(sparkSession: SparkSession, config: FilesWriterConfig): BasicFileOutputWriter = {
    if (sparkSession.sparkContext.isLocal && config.isInstanceOf[LocalFSWriterConfig]) {
      sparkSession.sparkContext.addSparkListener(new OutputFilesCleanupListener(sparkSession, config.outputDirPath, config.format))
    }
    BasicFileOutputWriter(sparkSession, config)
  }


  protected def buildSolrWriter(sparkSession: SparkSession, config: SolrWriterConfig): SolrWriter = {
    config.adjustSparkConfig(sparkSession)
    val clientBuilder = SolrClientBuilder().setDefaultCollection(config.collection)
      .setZKSolrChroot(config.zkSolrZNode)
      .setZKServers(config.zkServers)
    val clientBuilderWithSec = config.krbConfig
        .map(clientBuilder.setJaas)
        .getOrElse(clientBuilder)
    val client = clientBuilderWithSec.build()
    val foreachWriter = SolrForeachWriter(config)
    SolrWriter(foreachWriter, client)
  }
}

