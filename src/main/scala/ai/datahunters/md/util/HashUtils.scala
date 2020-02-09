package ai.datahunters.md.util

import java.security.MessageDigest
import java.util.zip.{CRC32, Checksum}

object HashUtils {

  lazy val HashExecutionMap: Map[String, Function[Array[Byte], (String, String)]] = Map(
    HashNameNorm(CRC32) -> ((c: Array[Byte]) => generateChecksum(CRC32, c)),
    HashNameNorm(MD5) -> ((c: Array[Byte]) => hashExecution(MD5)(c)),
    HashNameNorm(SHA_1) -> ((c: Array[Byte]) => hashExecution(SHA_1)(c)),
    HashNameNorm(SHA_224) -> ((c: Array[Byte]) => hashExecution(SHA_224)(c)),
    HashNameNorm(SHA_256) -> ((c: Array[Byte]) => hashExecution(SHA_256)(c)),
    HashNameNorm(SHA_384) -> ((c: Array[Byte]) => hashExecution(SHA_384)(c)),
    HashNameNorm(SHA_512) -> ((c: Array[Byte]) => hashExecution(SHA_512)(c))
  )

  val HashNameNorm: (String) => String = s => s.replace(DASH, EMPTY_STRING).toLowerCase()

  /**
    * Method covers generation of hash for methods:
    * MD5
    * SHA_1
    * SHA_224
    * SHA_256
    * SHA_384
    * SHA_512
    *
    * @param algorithm String name of algorithm
    * @param content file
    * @return
    */
  private def hashExecution(algorithm: String)(content: Array[Byte]) = {
    val hashResult = MessageDigest.getInstance(algorithm)
      .digest(content)
      .map(0xFF & _)
      .map("%02x".format(_))
      .foldLeft(EMPTY_STRING)(_ + _)
    HashNameNorm(algorithm) -> hashResult
  }

  /**
    * Method covers generation of hash for CRC32 method.
    *
    * @param algorithm String name of algorithm
    * @param content file
    * @return
    */
  private def generateChecksum(algorithm: String, content: Array[Byte]) = {
    val crc: Checksum = new CRC32()
    crc.update(content, 0, content.length)
    val hashResult = crc.getValue.intValue().toHexString
    HashNameNorm(algorithm) -> hashResult
  }
  val DASH = "-"
  val EMPTY_STRING = ""

  val CRC32 = "CRC32"
  val MD5 = "MD5"
  val SHA_1 = "SHA-1"
  val SHA_224 = "SHA-224"
  val SHA_256 = "SHA-256"
  val SHA_384 = "SHA-384"
  val SHA_512 = "SHA-512"
}
