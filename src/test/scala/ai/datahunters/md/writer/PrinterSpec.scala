package ai.datahunters.md.writer

import ai.datahunters.md.UnitSpec
import org.apache.spark.sql.DataFrame
import org.mockito.Mockito._

class PrinterSpec extends UnitSpec {

  "A Printer" should "show content of DataFrame" in {
    val df = mock[DataFrame]
    Printer.write(df)
    verify(df).show(Int.MaxValue, false)
  }
}
