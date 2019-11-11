package ai.datahunters.md.pipeline

import ai.datahunters.md.UnitSpec
import ai.datahunters.md.config.{ConfigLoader, SessionConfig}
import org.apache.spark.SparkException
import org.mockito.Mockito._

class SessionCreatorSpec extends UnitSpec {

  "A SessionCreator" should "create Spark Session for Standalone mode" in {
    val config = mock[SessionConfig]
    when(config.cores).thenReturn(7)
    when(config.maxMemoryGB).thenReturn(31)
    val sessionCreator = new SessionCreator(config, true, "MD test app")
    val session = sessionCreator.create()
    assert(session.sparkContext.master === "local[7]")
    assert(session.conf.get(ConfigLoader.SparkDriverMemKey) === "31g")
    assert(session.sparkContext.appName === "MD test app")
  }

  it should "throw exception due to lack of master in Distributed mode" in {
    val config = mock[SessionConfig]
    when(config.cores).thenReturn(7)
    when(config.maxMemoryGB).thenReturn(31)
    val sessionCreator = new SessionCreator(config, false, "MD test app")
    intercept[SparkException] {
      val session = sessionCreator.create()
      val rdd = session.sparkContext.parallelize(Seq()) // Execute some action to ensure all Spark initialization threads completion.
      rdd.collect()
    }
  }
}
