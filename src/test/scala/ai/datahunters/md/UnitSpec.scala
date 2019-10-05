package ai.datahunters.md

import java.nio.file.Paths

import org.scalatest._
import org.scalatestplus.mockito.MockitoSugar

class UnitSpec extends FlatSpec with Matchers with
  OptionValues with Inside with Inspectors with MockitoSugar {

  import UnitSpec._

  def configPath(relatedPath: String): String = s"$BaseConfigPath/$relatedPath"

  def imgPath(relatedPath: String): String = s"$BaseImgPath/$relatedPath"
}

object UnitSpec {
  val BasePath = getClass.getClassLoader.getResource(".").getPath
  val BaseImgPath = Paths.get(BasePath, "images")
  val BaseConfigPath = Paths.get(BasePath, "config")

}