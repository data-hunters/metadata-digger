package ai.datahunters.md.util

trait Parser[T] {
  def parse(input: String): Option[T]
}


object Parser {

  def parse[T](input: String)(implicit parser: Parser[T]): Option[T] =
    parser.parse(input)

  import util.Try

  implicit object BooleanParser extends Parser[Boolean] {
    def parse(input: String) = {
      val cleanInput = input.toLowerCase
        .replace("0", "false")
        .replace("1", "true")
      Try(cleanInput.toBoolean).toOption
    }
  }
}
