package ai.datahunters.md.util

import java.time.ZoneOffset
import java.util.Date

import ai.datahunters.md.UnitSpec

class DateTimeUtilsSpec extends UnitSpec {

  "A DateTimeUtils" should "convert datetime" in {
    val d = "2016-03-21 10:14:01"
    val actualDate = DateTimeUtils.parseExifDate(d)
    val actualDateCast = actualDate.get.toInstant().atOffset(ZoneOffset.UTC).toLocalDateTime()
    assert(actualDateCast.getYear === 2016)
    assert(actualDateCast.getMonth.getValue === 3)
    assert(actualDateCast.getDayOfMonth === 21)
    assert(actualDateCast.getHour === 10)
    assert(actualDateCast.getMinute === 14)
    assert(actualDateCast.getSecond === 1)
  }

  it should "convert date" in {
    val d = "2016-12-19"
    val actualDate = DateTimeUtils.parseExifDate(d)
    val actualDateCast = actualDate.get.toInstant().atOffset(ZoneOffset.UTC).toLocalDateTime()
    assert(actualDateCast.getYear === 2016)
    assert(actualDateCast.getMonth.getValue === 12)
    assert(actualDateCast.getDayOfMonth === 19)
    assert(actualDateCast.getHour === 0)
    assert(actualDateCast.getMinute === 0)
    assert(actualDateCast.getSecond === 0)
  }

  it should "convert datetime with subseconds" in {
    val d = "2016-03-21 10:14:01.123"
    val actualDate = DateTimeUtils.parseExifDate(d)
    val actualDateCast = actualDate.get.toInstant().atOffset(ZoneOffset.UTC).toLocalDateTime()
    assert(actualDateCast.getYear === 2016)
    assert(actualDateCast.getMonth.getValue === 3)
    assert(actualDateCast.getDayOfMonth === 21)
    assert(actualDateCast.getHour === 10)
    assert(actualDateCast.getMinute === 14)
    assert(actualDateCast.getSecond === 1)
    assert(actualDateCast.getNano === 123000000)
  }

  it should "convert datetime with time zone" in {
    val d = "2016-03-21T10:14:01+03:00"
    val actualDate = DateTimeUtils.parseExifDate(d)
    val actualDateCast = actualDate.get.toInstant().atOffset(ZoneOffset.UTC).toLocalDateTime()
    assert(actualDateCast.getYear === 2016)
    assert(actualDateCast.getMonth.getValue === 3)
    assert(actualDateCast.getDayOfMonth === 21)
    assert(actualDateCast.getHour === 7)
    assert(actualDateCast.getMinute === 14)
    assert(actualDateCast.getSecond === 1)

  }

}

object DateTimeUtilsSpec {


}