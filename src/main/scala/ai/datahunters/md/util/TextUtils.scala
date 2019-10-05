package ai.datahunters.md.util

object TextUtils {

  def safeName(v: String): String = v.replaceAll("[\\{\\}\\.\\(\\),'\"\\?;<>*&%$#@\\!` ]", "")

  def camelCase(v: String): String = v.split(" ")
    .map(_.capitalize).mkString("")

}
