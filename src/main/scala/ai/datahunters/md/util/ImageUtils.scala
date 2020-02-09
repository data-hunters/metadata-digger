package ai.datahunters.md.util
import com.intel.analytics.bigdl.transform.vision.image.augmentation.Resize
import com.intel.analytics.bigdl.transform.vision.image.opencv.OpenCVMat

object ImageUtils {

  def resizeImg(img: Array[Byte], maxWidth: Int, maxHeight: Int): Array[Byte] = {
    val imgMat = OpenCVMat.fromImageBytes(img)
    val width = imgMat.width().asInstanceOf[Float]
    val height = imgMat.height().asInstanceOf[Float]
    if (width > maxWidth || height > maxHeight) {
      val widthRatio = width / maxWidth.asInstanceOf[Float]
      val ratio = if (height / widthRatio > maxHeight) {
        height / maxHeight
      } else {
        widthRatio
      }
      Resize.transform(imgMat, imgMat, Math.round(width / ratio), Math.round(height / ratio))
      OpenCVMat.imencode(imgMat)
    } else {
      img
    }
  }
}
