package ai.datahunters.md.reader

import java.security.MessageDigest

import javax.xml.bind.DatatypeConverter
import org.apache.spark.sql.DataFrame

trait BinaryFilesReader extends PipelineSource {

}

object BinaryFilesReader {

  val MD5Alg = "MD5"

  /**
    * Generate hash from array of bytes using MD5 algorithm.
    *
    * @param value
    * @return
    */
  def md5sum(value: Array[Byte]): Array[Byte] = MessageDigest.getInstance(MD5Alg)
    .digest(value)

  /**
    * Generate hash from string using MD5 algorithm.
    * @param value
    * @return
    */
  def md5sum(value: String): Array[Byte] = md5sum(value.getBytes)

  /**
    * Convert array of bytes to String
    *
    * @param input
    * @return
    */
  implicit def bytesToStr(input: Array[Byte]): String = DatatypeConverter.printHexBinary(input)
}