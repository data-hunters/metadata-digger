package ai.datahunters.md.writer.solr

import java.time.{ZoneId, ZoneOffset}
import java.util.Date

import ai.datahunters.md.UnitSpec
import org.apache.solr.common.SolrInputDocument

class ConvertersSpec extends UnitSpec {

  import ConvertersSpec._

  "A Converters" should "convert date and add to SolrInputDocument" in {
    val doc = new SolrInputDocument()
    val d = "2016-01-02 12:34:01"
    assert(!doc.containsKey(SampleField))
    Converters.addDatetime(SampleField, doc)(d)
    val actualDate = doc.get(SampleField).getValue
    assert(actualDate.isInstanceOf[Date])
    val actualDateCast = actualDate.asInstanceOf[Date].toInstant().atOffset(ZoneOffset.UTC).toLocalDateTime()
    assert(actualDateCast.getYear === 2016)
    assert(actualDateCast.getMonth.getValue === 1)
    assert(actualDateCast.getDayOfMonth === 2)
    assert(actualDateCast.getHour === 12)
    assert(actualDateCast.getMinute === 34)
    assert(actualDateCast.getSecond === 1)
  }

  it should "not convert invalid date" in {
    val doc = new SolrInputDocument()
    val d = "b2016-01-02 asdasd"
    assert(!doc.containsKey(SampleField))
    Converters.addDatetime(SampleField, doc)(d)
    assert(!doc.containsKey(SampleField))
  }

  it should "convert to integer and add to SolrInputDocument" in {
    val doc = new SolrInputDocument()
    assert(!doc.containsKey(SampleField))
    Converters.addInt(SampleField, doc)("23")
    val actualInt = doc.get(SampleField).getValue
    assert(actualInt.isInstanceOf[Int])
    assert(actualInt === 23)
  }

  it should "not convert invalid integer" in {
    val doc = new SolrInputDocument()
    assert(!doc.containsKey(SampleField))
    Converters.addInt(SampleField, doc)("a")
    Converters.addInt(SampleField + "2", doc)("")
    val actualInt1 = doc.get(SampleField).getValue
    assert(actualInt1.isInstanceOf[Int])
    assert(actualInt1 === -1)
    val actualInt2 = doc.get(SampleField + "2").getValue
    assert(actualInt2.isInstanceOf[Int])
    assert(actualInt2 === -1)
  }
}

object ConvertersSpec {

  val SampleField = "some_field"
}
