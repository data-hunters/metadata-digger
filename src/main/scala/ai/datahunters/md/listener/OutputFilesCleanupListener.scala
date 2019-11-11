package ai.datahunters.md.listener

import java.nio.file.{Files, Path, Paths}

import ai.datahunters.md.config.Writer
import ai.datahunters.md.config.writer.{FilesWriterConfig, LocalFSWriterConfig}
import ai.datahunters.md.util.FilesHandler
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd}
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

/**
  * Spark Listener (intended for Standalone mode with local file system usage only) that is triggered after application completion to make Spark output results cleaner.
  * Spark produces some working files like _SUCCESS or *.crc and additionally final file's name is not very human-friendly.
  * This listener removes working files and changes name of all output files.
  *
  * @param sparkSession
  * @param outputDirPath
  * @param format
  */
class OutputFilesCleanupListener(sparkSession: SparkSession, outputDir: String, format: String, filesHandler: FilesHandler = new FilesHandler) extends SparkListener {

   if (!sparkSession.sparkContext.isLocal) {
    throw new RuntimeException("This output listener cannot be used in other than local mode!")
  }

  private val outputDirPath = outputDir.replace(LocalFSWriterConfig.PathPrefix, "")
  private val logger = LoggerFactory.getLogger(classOf[OutputFilesCleanupListener])
  private val extension = s".$format"
  private val outputDirName = Paths.get(outputDirPath)
    .getFileName
    .toString
  private val finalOutputDirPath = outputDirPath.replace(outputDirName, "")


  /**
    * Triggers action when whole application ends.
    *
    * @param applicationEnd
    */
  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    super.onApplicationEnd(applicationEnd)
    if (filesHandler.exists(outputDirPath)) {
      logger.info("Job completed, moving output files...")
      val jsonFiles = filesHandler.listFiles(outputDirPath, format)
        .zipWithIndex
      moveFiles(jsonFiles)
      logger.info(s"Removing temporary files from directory: ${outputDirPath}")
      filesHandler.deleteTempSparkFiles(outputDirPath)
    }
  }

  private def moveFiles(jsonFiles: Seq[(Path, Int)]): Unit = {
    if (jsonFiles.size == 1) {
      val filePath = jsonFiles.last._1
      moveFile(filePath)
    } else {
      jsonFiles.foreach(pathInfo => moveFile(pathInfo._1, Some(pathInfo._2)))
    }
  }

  private def moveFile(sourcePath: Path, index: Option[Int] = None): Unit = {
    val outputFileName = buildFileName(index)
    val outputPath = Paths.get(outputDirPath, outputFileName)
    logger.info(s"Moving temporary files from ${sourcePath} to ${outputPath}.")
    filesHandler.moveFile(sourcePath, outputPath)
  }

  private def buildFileName(index: Option[Int] = None): String = {
    val baseName = outputDirName
    index.map(i => s"${baseName}_${i}${extension}")
      .orElse(Some(s"${baseName}${extension}"))
      .get
  }






}
