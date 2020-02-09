package ai.datahunters.md.udf.image

import java.util.Base64

import ai.datahunters.md.util.ImageUtils
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

object ImageProcessing {

  case class ThumbnailSize(maxWidth: Int, maxHeight: Int)

  /**
    * UDF generating thumbnail based on original image. Generated image is converted to base64.
    * Input argument for UDF: column of type Array[Byte]
    * UDF output: String
    *
    * @param size
    * @return
    */
  def generateThumbnail(size: ThumbnailSize): UserDefinedFunction = {
    udf(Transformations.generateThumbnailT(size) _)
  }

  object Transformations {

    def generateThumbnailT(size: ThumbnailSize)(image: Array[Byte]): String = {
      val img = ImageUtils.resizeImg(image, size.maxWidth, size.maxHeight)
      new String(Base64.getEncoder().encode(img))
    }
  }
}
