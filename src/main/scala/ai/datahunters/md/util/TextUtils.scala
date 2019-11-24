package ai.datahunters.md.util

object TextUtils {


  def safeName(v: String): String = v.replaceAll("[\\{\\}\\.\\(\\),'\"\\?;:<>*&%$#@\\!`\\[\\]/]", "")


  object NamingConvention {

    def apply(conventionName: String): (String) => (String) = conventionName.toLowerCase match {
      case "camelcase" => camelCase
      case "snakecase" => snakeCase
      case other => throw new RuntimeException(s"Column Naming convention $other not supported")
    }
  }

  def camelCase(v: String): String = v.split(" ")
    .flatMap(_.split("-"))
    .map(_.capitalize).mkString("")

  def snakeCase(v: String): String = v.replace(" ", "_")
    .replace("-", "_")
    .toLowerCase

  /**
    * Calculate Levenshtein Distance.
    * Implementation from: https://rosettacode.org/wiki/Levenshtein_distance#Translated_Wikipedia_algorithm.
    *
    * @param s1
    * @param s2
    * @return
    */
  def levenshteinDistance(s1: String, s2: String): Int = {
    if (s1.equals(s2)) return 0
    val dist = Array.tabulate(s2.length + 1, s1.length + 1) { (j, i) => if (j == 0) i else if (i == 0) j else 0 }

    @inline
    def minimum(i: Int*): Int = i.min

    for {j <- dist.indices.tail
         i <- dist(0).indices.tail} dist(j)(i) =
      if (s2(j - 1) == s1(i - 1)) dist(j - 1)(i - 1)
      else minimum(dist(j - 1)(i) + 1, dist(j)(i - 1) + 1, dist(j - 1)(i - 1) + 1)

    dist(s2.length)(s1.length)
  }

  def isEmpty(str: String): Boolean = (str == null || str.isEmpty)

  def notEmpty(str: String): Boolean = !isEmpty(str)
}
