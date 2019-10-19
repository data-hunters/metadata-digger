package ai.datahunters.md.writer

import ai.datahunters.md.config.{FilesWriterConfig, SolrWriterConfig, WriterConfig}
import ai.datahunters.md.listener.OutputFilesCleanupListener
import ai.datahunters.md.writer.solr.{SolrClientBuilder, SolrForeachWriter}
import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession

object PipelineSinkFactory {



  def create(config: Config, sparkSession: SparkSession): PipelineSink = {
    val format = config.getString(WriterConfig.OutputFormatKey)
    format match {
      case FileOutputWriter.CsvFormat | FileOutputWriter.JsonFormat => buildBasicFileOutputWriter(sparkSession, config)
      case SolrWriter.FormatName => buildSolrWriter(sparkSession, config)
    }
  }

  protected def buildBasicFileOutputWriter(sparkSession: SparkSession, config: Config): BasicFileOutputWriter = {
    val c = FilesWriterConfig.build(config)
    if (sparkSession.sparkContext.isLocal) {
      sparkSession.sparkContext.addSparkListener(new OutputFilesCleanupListener(sparkSession, c.outputDirPath, c.format))
    }
    BasicFileOutputWriter(sparkSession, c.format, c.outputFilesNum, c.outputDirPath)
  }

  protected def buildSolrWriter(sparkSession: SparkSession, config: Config): SolrWriter = {
    val c = SolrWriterConfig.build(config)
    val client = SolrClientBuilder().setDefaultCollection(c.collection)
      .setZKSolrChroot(c.zkSolrZNode)
      .setZKServers(c.zkServers)
      .build()
    val foreachWriter = SolrForeachWriter(c.zkServers, c.zkSolrZNode, c.collection)
    SolrWriter(foreachWriter, client)
  }
}
