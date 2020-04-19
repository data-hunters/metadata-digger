package ai.datahunters.md.util

trait Parser[T] {
  def parse(input: String): T
}

object Parser {

  val NonFloatCharactersRegex = "[^\\d\\.\\-]"

  def parse[T](input: String)(implicit parser: Parser[T]): T = {
    parser.parse(input)
  }

  implicit object IntParser extends Parser[Int] {
    override def parse(input: String): Int = cleanAndParseInt(input)
  }

  implicit object LongParser extends Parser[Long] {
    override def parse(input: String): Long = cleanAndParseLong(input)
  }

  implicit object FloatParser extends Parser[Float] {
    override def parse(input: String): Float = cleanAndParseFloat(input)
  }

  implicit object BigDecimalParser extends Parser[BigDecimal] {
    override def parse(input: String): BigDecimal = cleanAndParseBigDecimal(input)
  }


  protected def cleanAndParseInt(value: String): Int = {
    cleanAndParseBigDecimal(value).toInt
  }

  protected def cleanAndParseLong(value: String): Long = {
    cleanAndParseBigDecimal(value).toLong
  }

  protected def cleanAndParseFloat(value: String): Float = {
    value.replaceAll(NonFloatCharactersRegex, "").toFloat
  }

  protected def cleanAndParseBigDecimal(value: String): BigDecimal = {
    BigDecimal(value.replaceAll(NonFloatCharactersRegex, ""))
  }
}