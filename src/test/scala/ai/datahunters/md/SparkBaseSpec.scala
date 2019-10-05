package ai.datahunters.md

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Suite}

trait SparkBaseSpec extends BeforeAndAfterAll {

  this: Suite =>

  protected var sparkSession: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    val conf = new SparkConf()
      .setMaster("local[1]")
      .setAppName(this.getClass.getSimpleName)
      .set("spark.sql.shuffle.partitions", "1")

    sparkConfig.foreach { case (k, v) => conf.setIfMissing(k, v) }

    sparkSession = SparkSession.builder()
      .appName("Testing Spark module")
      .config(conf)
      .getOrCreate()


  }

  def sparkConfig: Map[String, String] = Map.empty

  override def afterAll(): Unit = {
    if (sparkSession != null) {
      sparkSession.stop()
      sparkSession = null
    }
    super.afterAll()
  }

  def spark: SparkSession = sparkSession
}
