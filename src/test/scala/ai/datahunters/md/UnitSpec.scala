package ai.datahunters.md

import java.nio.file.Paths

import org.scalatest._
import org.scalatestplus.mockito.MockitoSugar

class UnitSpec extends FlatSpec with Matchers with
  OptionValues with Inside with Inspectors with MockitoSugar {

  import UnitSpec._

  def configPath(relatedPath: String): String = s"$BaseConfigPath/$relatedPath"

  def imgPath(relatedPath: String): String = s"$BaseImgPath/$relatedPath"

  def otherImgPath(relatedPath: String): String = s"$BaseOtherImgPath/$relatedPath"

  def modelPath(relatedPath: String): String = s"$BaseModelPath/$relatedPath"
}

object UnitSpec {
  val BasePath = getClass.getClassLoader.getResource(".").getPath
  val BaseImgPath = Paths.get(BasePath, "images")
  val BaseOtherImgPath = Paths.get(BasePath, "other_images")
  val BaseConfigPath = Paths.get(BasePath, "config")
  val BaseModelPath = Paths.get(BasePath, "test_models")
}