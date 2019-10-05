package ai.datahunters.md.util

import java.nio.file.Paths

import ai.datahunters.md.UnitSpec
import ai.datahunters.md.UnitSpec.BasePath

class FilesHandlerSpec extends UnitSpec {
  import UnitSpec._
  import FilesHandlerSpec._

  "A FilesHandler" should "list all files in directory" in {
    val handler = new FilesHandler
    val files = handler.listFiles(TestDir, "json").map(_.toString).sorted
    val expectedFiles = Seq(Paths.get(TestDir, "sample1.json"), Paths.get(TestDir, "sample2.json")).map(_.toString).sorted
    assert(expectedFiles === files)
  }

  it should "properly verify if file exits or not" in {
    val handler = new FilesHandler
    val dir = path("listing_test")
    val notExistingDir = path("listing_ttest")
    assert(handler.exists(dir))
    assert(!handler.exists(notExistingDir))
  }


}

object FilesHandlerSpec {

  val TestDir = path("listing_test")

  private def path(fileName: String): String = Paths.get(BasePath, fileName).toString
}
