package ai.datahunters.md.workflow

import java.nio.file.{Files, Paths}

import ai.datahunters.md.reader.PipelineSource
import ai.datahunters.md.schema.BinaryInputSchemaConfig
import ai.datahunters.md.{SparkBaseSpec, UnitSpec}
import org.apache.spark.sql.Row
import org.mockito.Mockito._

class DescribeMetadataWorkflowSpec extends UnitSpec with SparkBaseSpec {

  import DescribeMetadataWorkflowSpec._

  "A DescribeMetadataWorkflow" should "run all elements in appropriate order" in {
    val imgBytes = Files.readAllBytes(Paths.get(imgPath(ImagePath)))
    val inputData = Seq(
      Row.fromTuple("somehash", "some/path", "some/path/img.jpg", imgBytes)
    )
    val rdd = sparkSession.sparkContext.parallelize(inputData)
    val readerOutputDF = sparkSession.createDataFrame(rdd, BinaryInputSchemaConfig().schema())
    val reader = mock[PipelineSource]
    when(reader.load()).thenReturn(readerOutputDF)
    val workflow = DescribeMetadataWorkflow(reader)
    workflow.run()
    verify(reader).load()
  }

}

object DescribeMetadataWorkflowSpec {
  val ImagePath = "landscape-4518195_960_720_pixabay_license.jpg"
}