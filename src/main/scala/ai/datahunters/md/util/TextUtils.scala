package ai.datahunters.md.util

object TextUtils {

  def safeName(v: String): String = v.replaceAll("[\\{\\}\\.\\(\\),'\"\\?;<>*&%$#@\\!`\\[\\]]", "")


  object NamingConvention {

    def apply(conventionName: String): (String) => (String) = conventionName.toLowerCase match {
      case "camelcase" => camelCase
      case "snakecase" => snakeCase
      case other => throw new RuntimeException(s"Column Naming convention $other not supported")
    }
  }

  def camelCase(v: String): String = v.split(" ")
    .map(_.capitalize).mkString("")

  def snakeCase(v: String): String = v.replace(" ", "_")
    .toLowerCase

}
