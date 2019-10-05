package ai.datahunters.md.util

import java.io.File
import java.nio.file.{Files, Path, Paths}
import java.util.stream.Collectors

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

  val CrcFilesExtension = ".crc"
  val SuccessTempFile = "_SUCCESS"

  def extension(format: String): String = s".$format"

}