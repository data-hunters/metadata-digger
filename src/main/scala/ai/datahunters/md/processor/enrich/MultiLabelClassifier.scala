package ai.datahunters.md.processor.enrich

import ai.datahunters.md.processor.Processor
import ai.datahunters.md.schema.MultiLabelPredictionSchemaConfig
import ai.datahunters.md.util.ImageFeatureUtils
import com.intel.analytics.bigdl.Module
import com.intel.analytics.bigdl.transform.vision.image.augmentation.{ChannelNormalize, Resize}
import com.intel.analytics.bigdl.transform.vision.image.{FeatureTransformer, ImageFrameToSample, MatToTensor}
import org.apache.spark.sql.{DataFrame, SparkSession}

case class MultiLabelClassifier(@transient sparkSession: SparkSession,
                                @transient model: Module[Float],
                                labelsMapping: Map[Int, String],
                                threshold: Float,
                                targetImgWidh: Int = 256,
                                targetImgHeight: Int = 256) extends Processor {

  protected val featureTransformer: FeatureTransformer = Resize(targetImgWidh, targetImgHeight) ->
    ChannelNormalize(0, 0, 0, 1, 1, 1) ->
    MatToTensor[Float]() ->
    ImageFrameToSample[Float]()


  override def execute(inputDF: DataFrame): DataFrame = {
    val imgFrame = ImageFeatureUtils.buildImageFrame(inputDF, labelsMapping.size, featureTransformer)
    val predictionsRDD = model.predictImage(imgFrame)
      .toDistributed()
      .rdd
      .map(ImageFeatureUtils.imageFeatureToRow(labelsMapping, threshold = threshold))
    sparkSession.createDataFrame(predictionsRDD, MultiLabelPredictionSchemaConfig.schema())
  }


}
