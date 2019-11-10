package ai.datahunters.md.writer.solr

import ai.datahunters.md.util.DateTimeUtils
import org.apache.solr.common.SolrInputDocument

object Converters {

  val UnknownIntValue = -1
  val PixelsUnit = "pixels"
  val InchesUnit = "inches"

  def addDatetime(name: String, doc: SolrInputDocument)(value: String): Unit = {
    val dt = DateTimeUtils.parseExifDate(value)
    dt.foreach(dt => {
      doc.addField(name, dt)
    })
  }

  def addInt(name: String, doc: SolrInputDocument)(value: String): Unit = {
    try {
      val intVal = value.replace(PixelsUnit, "")
        .replace(InchesUnit, "")
        .trim.toInt
      doc.addField(name, intVal)
    } catch {
      case e: NumberFormatException => doc.addField(name, UnknownIntValue)
    }
  }

}
