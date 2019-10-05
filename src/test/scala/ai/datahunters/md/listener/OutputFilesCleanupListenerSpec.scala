package ai.datahunters.md.listener

import java.nio.file.Paths

import ai.datahunters.md.util.FilesHandler
import ai.datahunters.md.{SparkBaseSpec, UnitSpec}
import org.apache.spark.scheduler.SparkListenerApplicationEnd
import org.mockito.Mockito._
import org.mockito.ArgumentMatchers.any

class OutputFilesCleanupListenerSpec extends UnitSpec with SparkBaseSpec {
  import OutputFilesCleanupListenerSpec._

  "An OutputFilesCleanupListener" should "call appropriate files operations when onApplicationEnd is triggered" in {

    val jsonPaths = Seq(Paths.get(JsonPath1))
    val outputDirPath = Paths.get(OutputDir)
    val filesHandler = mock[FilesHandler]
    val sparkAppEnd = mock[SparkListenerApplicationEnd]
    when(filesHandler.exists(OutputDir)).thenReturn(true)
    when(filesHandler.listFiles(OutputDir, Format)).thenReturn(jsonPaths)
    val listener = new OutputFilesCleanupListener(sparkSession, OutputDir, "json", filesHandler)
    listener.onApplicationEnd(sparkAppEnd)
    verify(filesHandler, times(1)).exists(OutputDir)
    verify(filesHandler, times(1)).listFiles(OutputDir, Format)
    verify(filesHandler, times(1)).moveFile(Paths.get(JsonPath1), Paths.get(s"$OutputDir/${OutputDir}.${Format}"))
    verify(filesHandler, times(1)).deleteTempSparkFiles(OutputDir)
  }

  it should "call appropriate files operations when onApplicationEnd is triggered and two output files with results have been produced" in {
    val jsonPaths = Seq(Paths.get(JsonPath1), Paths.get(JsonPath2))
    val outputDirPath = Paths.get(OutputDir)
    val filesHandler = mock[FilesHandler]
    val sparkAppEnd = mock[SparkListenerApplicationEnd]
    when(filesHandler.exists(OutputDir)).thenReturn(true)
    when(filesHandler.listFiles(OutputDir, Format)).thenReturn(jsonPaths)
    val listener = new OutputFilesCleanupListener(sparkSession, OutputDir, "json", filesHandler)
    listener.onApplicationEnd(sparkAppEnd)
    verify(filesHandler, times(1)).exists(OutputDir)
    verify(filesHandler, times(1)).listFiles(OutputDir, Format)
    verify(filesHandler, times(2)).moveFile(any(), any())
    verify(filesHandler, times(1)).moveFile(Paths.get(JsonPath1), Paths.get(s"$OutputDir/${OutputDir}_0.${Format}"))
    verify(filesHandler, times(1)).moveFile(Paths.get(JsonPath2), Paths.get(s"$OutputDir/${OutputDir}_1.${Format}"))
    verify(filesHandler, times(1)).deleteTempSparkFiles(OutputDir)
  }
}

object OutputFilesCleanupListenerSpec {

  val OutputDir = "tmp"
  val Format = "json"
  val JsonPath1 = s"$OutputDir/test1.json"
  val JsonPath2 = s"$OutputDir/test2.json"
}