package ai.datahunters.md.util

import java.text.{ParseException, SimpleDateFormat}
import java.util.regex.Pattern
import java.util.{Calendar, Date, TimeZone}

import scala.annotation.tailrec

object DateTimeUtils {

  val DefaultTimeZone = "GMT"
  val MainDateTimeFormat = "yyyy:MM:dd HH:mm:ss"
  val MainDateTimeFormatter = new SimpleDateFormat(MainDateTimeFormat)

  // This seems to cover all known Exif and Xmp date strings
  // Note that "    :  :     :  :  " is a valid date string according to the Exif spec (which means 'unknown date'): http://www.awaresystems.be/imaging/tiff/tifftags/privateifd/exif/datetimeoriginal.html
  val ExifDatePatterns = Seq(
    MainDateTimeFormat,
    "yyyy:MM:dd HH:mm",
    "yyyy-MM-dd HH:mm:ss",
    "yyyy-MM-dd HH:mm",
    "yyyy.MM.dd HH:mm:ss",
    "yyyy.MM.dd HH:mm",
    "yyyy-MM-dd'T'HH:mm:ss",
    "yyyy-MM-dd'T'HH:mm",
    "yyyy-MM-dd",
    "yyyy-MM",
    "yyyyMMdd", // as used in IPTC data
    "yyyy"
  )

  // if the date string has subsecond information, it supersedes the subsecond parameter
  private val SubsecondPattern = Pattern.compile("(\\d\\d:\\d\\d:\\d\\d)(\\.\\d+)")

  // if the date string has time zone information, it supersedes the timeZone parameter
  private val TimeZonePattern = Pattern.compile("(Z|[+-]\\d\\d:\\d\\d)$")


  /**
    * Convert string value of tag to java.util.Date
    * Based on converter from com.drewnoakes:metadata-extractor:2.12.0 com.drew.metadata.Directory#getDate
    *
    * @param dt
    * @return
    */
  def parseExifDate(dt: String): Option[Date] = {
    val subsecondMatcher = SubsecondPattern.matcher(dt)
    var dateString = dt
    var subsecond: Option[String] = None
    if (subsecondMatcher.find()) {
      subsecond = Some(subsecondMatcher.group(2).substring(1))
      dateString = subsecondMatcher.replaceAll("$1")
    }

    val timeZoneMatcher = TimeZonePattern.matcher(dateString)
    var timeZone: Option[TimeZone] = None
    if (timeZoneMatcher.find()) {
      timeZone = Some(TimeZone.getTimeZone(DefaultTimeZone + timeZoneMatcher.group().replaceAll("Z", "")))
      dateString = timeZoneMatcher.replaceAll("")
    }

    val date = parse(dateString, timeZone, ExifDatePatterns.iterator)
    return subsecond.map(handleSubseconds(date)).getOrElse(date)
  }

  @tailrec private def parse(dateString: String, timeZone: Option[TimeZone], datePatternIt: Iterator[String]): Option[Date] = {
    try {
      val parser = new SimpleDateFormat(datePatternIt.next())
      parser.setTimeZone(timeZone.getOrElse(TimeZone.getTimeZone(DefaultTimeZone)))
      Some(parser.parse(dateString))
    } catch {
      case e: ParseException => if (datePatternIt.hasNext) {
        parse(dateString, timeZone, datePatternIt) // Try next date pattern
      } else {
        None
      }
    }
  }

  private def handleSubseconds(date: Option[Date])(subsecond: String): Option[Date] = {
    try {
      val millisecond: Int = (("." + subsecond).toDouble * 1000).toInt
      if (millisecond >= 0 && millisecond < 1000) {
        val calendar = Calendar.getInstance();
        return date.map(d => {
          calendar.setTime(d)
          calendar.set(Calendar.MILLISECOND, millisecond)
          calendar.getTime
        })
      }
    } catch {
      case e: NumberFormatException => return date
    }
    return date
  }

}
