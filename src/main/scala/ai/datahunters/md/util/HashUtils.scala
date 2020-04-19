package ai.datahunters.md.util

import java.security.MessageDigest
import java.util.zip.{CRC32, Checksum}

object HashUtils {

  lazy val HashExecutionMap: Map[String, Function[Array[Byte], (String, String)]] = Map(
    HashNameNorm(Crc32) -> ((c: Array[Byte]) => generateChecksum(Crc32, c)),
    HashNameNorm(Md5) -> ((c: Array[Byte]) => hashExecution(Md5)(c)),
    HashNameNorm(Sha1) -> ((c: Array[Byte]) => hashExecution(Sha1)(c)),
    HashNameNorm(Sha224) -> ((c: Array[Byte]) => hashExecution(Sha224)(c)),
    HashNameNorm(Sha256) -> ((c: Array[Byte]) => hashExecution(Sha256)(c)),
    HashNameNorm(Sha384) -> ((c: Array[Byte]) => hashExecution(Sha384)(c)),
    HashNameNorm(Sha512) -> ((c: Array[Byte]) => hashExecution(Sha512)(c))
  )

  val HashNameNorm: (String) => String = s => s.replace(Dash, EmptyString).toLowerCase()

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
      .foldLeft(EmptyString)(_ + _)
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
  val Dash = "-"
  val EmptyString = ""

  val Crc32 = "CRC32"
  val Md5 = "MD5"
  val Sha1 = "SHA-1"
  val Sha224 = "SHA-224"
  val Sha256 = "SHA-256"
  val Sha384 = "SHA-384"
  val Sha512 = "SHA-512"
}
