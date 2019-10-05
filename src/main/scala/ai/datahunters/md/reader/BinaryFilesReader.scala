package ai.datahunters.md.reader

import java.security.MessageDigest

import javax.xml.bind.DatatypeConverter
import org.apache.spark.sql.DataFrame

trait BinaryFilesReader {

  def read(path: String): DataFrame

  def read(paths: Seq[String]): DataFrame

}

object BinaryFilesReader {

  val MD5Alg = "MD5"

  def md5sum(value: String): Array[Byte] = MessageDigest.getInstance(MD5Alg)
    .digest(value.getBytes)

  implicit def bytesToStr(input: Array[Byte]): String = DatatypeConverter.printHexBinary(input)
}