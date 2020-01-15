package ai.datahunters.md.util

import java.security.MessageDigest
import java.util.zip.{CRC32, Checksum}

object HashUtils {

  val HashExecutionMap: Map[String, Function[Array[Byte], (String, String)]] = Map(
    "crc32" -> ((c: Array[Byte]) => generateChecksum(c)),
    "md5" -> ((c: Array[Byte]) => hashExecution("MD5")(c)),
    "sha1" -> ((c: Array[Byte]) => hashExecution("SHA-1")(c)),
    "sha224" -> ((c: Array[Byte]) => hashExecution("SHA-224")(c)),
    "sha256" -> ((c: Array[Byte]) => hashExecution("SHA-256")(c)),
    "sha384" -> ((c: Array[Byte]) => hashExecution("SHA-384")(c)),
    "sha512" -> ((c: Array[Byte]) => hashExecution("SHA-512")(c))
  )

  private def hashExecution(algorithm: String)(content: Array[Byte]) = {
    val hashResult = MessageDigest.getInstance(algorithm)
      .digest(content)
      .map(0xFF & _)
      .map("%02x".format(_))
      .foldLeft("")(_ + _)
    algorithm -> hashResult
  }

  private def generateChecksum(content: Array[Byte]) = {
    val crc: Checksum = new CRC32()
    crc.update(content, 0, content.length)
    val hashResult = crc.getValue.intValue().toHexString
    "CRC32" -> hashResult
  }
}
