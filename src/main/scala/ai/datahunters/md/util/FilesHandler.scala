package ai.datahunters.md.util

import java.io.File
import java.nio.file.{Files, Path, Paths}
import java.util.stream.Collectors

import org.slf4j.LoggerFactory

/**
  * Wrapper for common IO operations included more specific to Spark.
  */
class FilesHandler {
  import scala.collection.JavaConversions._
  import FilesHandler._

  /**
    * List all files with specific format.
    * @param outputDirPath
    * @param format
    * @return
    */
  def listFiles(outputDirPath:String, format: String): Seq[Path] = Files.list(Paths.get(outputDirPath))
      .collect(Collectors.toList())
      .toSeq
      .filter(_.getFileName.toString.endsWith(extension(format)))


  /**
    * Removes all Spark working files that are stored in output directory.
    *
    * @param outputDirPath
    */
  def deleteTempSparkFiles(outputDirPath: String): Unit = {
    new File(outputDirPath).listFiles()
      .toSeq
      .filter(_.getName.endsWith(CrcFilesExtension))
      .foreach(_.delete)
    Files.deleteIfExists(Paths.get(outputDirPath, SuccessTempFile))
  }

  /**
    * Moves file from one location to another.
    * @param sourcePath
    * @param outputPath
    */
  def moveFile(sourcePath: Path, outputPath: Path): Unit = Files.move(sourcePath, outputPath)

  /**
    * Check if file/directory exists.
    * @param path
    * @return
    */
  def exists(path: String): Boolean = Files.exists(Paths.get(path))

}

object FilesHandler {

  private val Logger = LoggerFactory.getLogger(classOf[FilesHandler])
  val CrcFilesExtension = ".crc"
  val SuccessTempFile = "_SUCCESS"

  def extension(format: String): String = s".$format"

  /**
    * Adjust paths by forcing prefix related to particular type of storage
    *
    * @param pathPrefix
    * @param storageName
    * @param paths
    * @return
    */
  def fixPaths(pathPrefix: String, storageName: String)(paths: Seq[String]): Seq[String] = {
    val fixPathFunc = fixPath(pathPrefix, storageName)(_)
    paths.map(fixPathFunc)
  }

  /**
    * Adjust path by forcing prefix related to particular type of storage
    *
    * @param pathPrefix
    * @param storageName
    * @param path
    * @return
    */
  def fixPath(pathPrefix: String, storageName: String)(path: String): String = if (path.startsWith(pathPrefix)) {
    path
  } else {
    val correctPath = s"$pathPrefix$path"
    Logger.warn(s"Path has to start with $pathPrefix for ${TextUtils.camelCase(storageName)} Storage, changing path from $path to $correctPath")
    correctPath
  }
}