package ai.datahunters.md.util

import java.io.{ByteArrayInputStream, File}
import java.nio.file.{Files, Paths}

import ai.datahunters.md.UnitSpec
import javax.imageio.ImageIO

class ImageUtilsSpec extends UnitSpec {

  "An ImageUtil" should "resize image" in {
    val img = Files.readAllBytes(Paths.get(imgPath("landscape-4518195_960_720_pixabay_license.jpg")))
    val originalBI = ImageIO.read(new ByteArrayInputStream(img))
    assert(originalBI.getWidth === 960)
    assert(originalBI.getHeight === 639)
    val resizedImg = ImageUtils.resizeImg(img, 800, 600)
    val bi = ImageIO.read(new ByteArrayInputStream(resizedImg))
    assert(bi.getWidth === 800)
    assert(bi.getHeight === 533)
  }
}
