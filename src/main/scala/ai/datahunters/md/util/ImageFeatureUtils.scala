package ai.datahunters.md.util

import java.awt.Color
import java.awt.image.BufferedImage
import java.io.{ByteArrayInputStream, ByteArrayOutputStream, File}

import ai.datahunters.md.schema.BinaryInputSchemaConfig
import com.intel.analytics.bigdl.tensor.Tensor
import com.intel.analytics.bigdl.transform.vision.image.opencv.OpenCVMat
import com.intel.analytics.bigdl.transform.vision.image.{FeatureTransformer, ImageFeature, ImageFrame}
import javax.imageio.ImageIO
import org.apache.spark.sql.{DataFrame, Row}
import org.slf4j.LoggerFactory

object ImageFeatureUtils {

  val UnknownLabel = "Unknown"
  val JPGType = "jpg"
  private val Logger = LoggerFactory.getLogger(ImageFeatureUtils.getClass)

  /**
    * Build ImageFrame from DataFrame by creating ImageFeature objects per Row.
    * Checks if image has appropriate number of channels and convert if not.
    *
    * @param inputDF
    * @param labelsSize
    * @param featureTransformer
    * @param idCol
    * @param fileDataCol
    * @param basePathCol
    * @param filePathCol
    * @return
    */
  def buildImageFrame(inputDF: DataFrame,
                      labelsSize: Int,
                      featureTransformer: FeatureTransformer,
                      idCol: String = BinaryInputSchemaConfig.IDCol,
                      fileDataCol: String = BinaryInputSchemaConfig.FileCol,
                      basePathCol: String = BinaryInputSchemaConfig.BasePathCol,
                      filePathCol: String = BinaryInputSchemaConfig.FilePathCol,
                      hashList: Seq[String] = Seq()): ImageFrame = {

    val inputRDD = inputDF
      .rdd
      .map(row => {
        prepareImageFeature(row.getAs(fileDataCol),
          row.getAs(idCol),
          row.getAs(basePathCol),
          row.getAs(filePathCol),
          Tensor(Array.fill(labelsSize)(0.0f), Array(labelsSize)),
          idCol,
          basePathCol,
          hashList.map(s => (s, row.getAs(s))))
      }).filter(_.isDefined)
      .map(_.get)
    ImageFrame.rdd(inputRDD).transform(featureTransformer)
  }

  /**
    * Convert ImageFeature to Row by retrieving all necessary values using keys.
    *
    * @param labelsMapping
    * @param idKey
    * @param basePathKey
    * @param threshold
    * @param imageFeature
    * @return
    */
  def imageFeatureToRow(labelsMapping: Map[Int, String],
                        idKey: String = BinaryInputSchemaConfig.IDCol,
                        basePathKey: String = BinaryInputSchemaConfig.BasePathCol,
                        threshold: Float = 0.5f,
                        hashList: Seq[String] = Seq())(imageFeature: ImageFeature): Row = {

    val predictions = imageFeature[Tensor[Float]](ImageFeature.predict).toArray()
      .zipWithIndex
      .filter(_._1 > threshold)
      .map(pred => labelsMapping.get(pred._2).getOrElse(UnknownLabel))
    val hashValues = hashList.map(s => imageFeature[String](s))
    Row.fromSeq(Seq(
      imageFeature(idKey),
      imageFeature(basePathKey),
      imageFeature(ImageFeature.uri),
      imageFeature[Array[Byte]](ImageFeature.bytes),
      predictions
    ) ++ hashValues
    )
  }

  private[util] def prepareImageFeature(originalImg: Array[Byte],
                                        id: String,
                                        basePath: String,
                                        filePath: String,
                                        label: Tensor[Float],
                                        idCol: String = BinaryInputSchemaConfig.IDCol,
                                        basePathCol: String = BinaryInputSchemaConfig.BasePathCol,
                                        hashMap: Seq[(String, String)] = Seq()): Option[ImageFeature] = {
    try {
      val bm = OpenCVMat.fromImageBytes(originalImg)
      val finalBM = if (bm.channels() != 3) {
        Logger.info(s"Image ${filePath} has not supported size of channel: ${bm.channels()}. Converting to: 3.")
        val converted = convertToRGB(originalImg)
        OpenCVMat.fromImageBytes(converted)
      } else {
        bm
      }
      val imgFeature = ImageFeature(originalImg, label, filePath)
      imgFeature.update(ImageFeature.mat, finalBM)
      imgFeature.update(idCol, id)
      imgFeature.update(basePathCol, basePath)
      hashMap.foreach(s => imgFeature.update(s._1, s._2))
      Some(imgFeature)
    } catch {
      case e: Exception => {
        Logger.warn(s"File ${filePath} could not be recognised as image (message: ${e}). Ignoring...")
        None
      }
    }
  }


  private[util] def convertToRGB(img: Array[Byte]): Array[Byte] = {
    val bi = ImageIO.read(new ByteArrayInputStream(img))
    val newBI = new BufferedImage(bi.getWidth, bi.getHeight, BufferedImage.TYPE_INT_RGB)
    val g = newBI.createGraphics()
    g.setColor(Color.WHITE)
    g.fillRect(0, 0, bi.getWidth(), bi.getHeight())
    g.drawImage(bi, 0, 0, null)
    g.dispose()
    val bos = new ByteArrayOutputStream()
    ImageIO.write(newBI, JPGType, bos)
    val outputImg = bos.toByteArray
    bos.close()
    outputImg
  }

}