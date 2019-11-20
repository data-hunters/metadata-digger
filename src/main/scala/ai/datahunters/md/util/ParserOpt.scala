package ai.datahunters.md.util

trait ParserOpt[T] {
  def parse(input: String): Option[T]
}


object ParserOpt {

  import util.Try

  def parse[T](input: String)(implicit parser: ParserOpt[T]): Option[T] =
    parser.parse(input)


  implicit object BooleanParserOpt$ extends ParserOpt[Boolean] {
    def parse(input: String) = {
      val cleanInput = input.toLowerCase
        .replace("0", "false")
        .replace("1", "true")
      Try(cleanInput.toBoolean).toOption
    }
  }

}
