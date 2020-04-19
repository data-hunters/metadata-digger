package ai.datahunters.md.processor.enrich

import java.nio.file.{Files, Paths}

import ai.datahunters.md.schema.{BinaryInputSchemaConfig, MultiLabelPredictionSchemaConfig}
import ai.datahunters.md.{SparkBaseSpec, UnitSpec}
import com.intel.analytics.bigdl.transform.vision.image.ImageFrame
import com.intel.analytics.bigdl.utils.Engine
import com.intel.analytics.zoo.pipeline.api.Net
import org.apache.spark.sql.Row

class MultiLabelClassifierSpec extends UnitSpec with SparkBaseSpec{

  import MultiLabelClassifierSpec._

  "A MultiLabelClassifier" should "classify images from DataFrame" in {
    Engine.init
    val testModel = Net.load[Float](modelPath("lenet_based"))
    val fileName = "000000547886_coco_dataset.jpg"
    val filePath = imgPath(fileName)
    val imgFrame = ImageFrame.read(imgPath(""), spark.sparkContext)
    val classifier = MultiLabelClassifier(spark, testModel, LabelsMapping, 0.5f)
    val input = Seq(
      Row.fromTuple("id1", imgPath(""), filePath, Files.readAllBytes(Paths.get(filePath)))
    )
    val rdd = spark.sparkContext.parallelize(input)
    val df = spark.createDataFrame(rdd, new BinaryInputSchemaConfig().schema())
    val predictions = classifier.execute(df)
    assert(predictions.columns === MultiLabelPredictionSchemaConfig().columns())
    val predictionsArr = predictions.drop(BinaryInputSchemaConfig.FileCol).collect()
    assert(predictionsArr.size === 1)
    val row = predictionsArr(0)
    assert(row.getAs[String](BinaryInputSchemaConfig.IDCol) === "id1")
    assert(row.getAs[String](BinaryInputSchemaConfig.BasePathCol) === imgPath(""))
    assert(row.getList[String](row.schema.fieldIndex(MultiLabelPredictionSchemaConfig().LabelsCol)).toArray === Array("person"))
  }
}

object MultiLabelClassifierSpec {

  val LabelsMapping = Map(
    69-> "oven",
    0-> "person",
    5-> "bus",
    10-> "fire hydrant",
    56-> "chair",
    42-> "fork",
    24-> "backpack",
    37-> "surfboard",
    25-> "umbrella",
    52-> "hot dog",
    14-> "bird",
    20-> "elephant",
    46-> "banana",
    57-> "couch",
    78-> "hair drier",
    29-> "frisbee",
    61-> "toilet",
    1-> "bicycle",
    74-> "clock",
    6-> "train",
    60-> "dining table",
    28-> "suitcase",
    38-> "tennis racket",
    70-> "toaster",
    21-> "bear",
    33-> "kite",
    65-> "remote",
    9-> "traffic light",
    53-> "pizza",
    77-> "teddy bear",
    13-> "bench",
    41-> "cup",
    73-> "book",
    2-> "car",
    32-> "sports ball",
    34-> "baseball bat",
    45-> "bowl",
    64-> "mouse",
    17-> "horse",
    22-> "zebra",
    44-> "spoon",
    59-> "bed",
    27-> "tie",
    71-> "sink",
    12-> "parking meter",
    54-> "donut",
    49-> "orange",
    76-> "scissors",
    7-> "truck",
    39-> "bottle",
    66-> "keyboard",
    3-> "motorcycle",
    35-> "baseball glove",
    48-> "sandwich",
    63-> "laptop",
    18-> "sheep",
    50-> "broccoli",
    67-> "cell phone",
    16-> "dog",
    31-> "snowboard",
    11-> "stop sign",
    72-> "refrigerator",
    43-> "knife",
    40-> "wine glass",
    26-> "handbag",
    55-> "cake",
    23-> "giraffe",
    8-> "boat",
    75-> "vase",
    58-> "potted plant",
    36-> "skateboard",
    30-> "skis",
    51-> "carrot",
    19-> "cow",
    4-> "airplane",
    79-> "toothbrush",
    47-> "apple",
    15-> "cat",
    68-> "microwave",
    62-> "tv"
  )
}
