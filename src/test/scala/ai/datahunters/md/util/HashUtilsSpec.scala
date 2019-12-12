package ai.datahunters.md.util

import ai.datahunters.md.UnitSpec

class HashUtilsSpec extends UnitSpec {

  import HashUtilsSpec._

  "a HashExecutionMap" should "generate CRC32 checksum" in {
    val crcResult = HashUtils.HashExecutionMap.get("crc32").map(f => f.apply(inputArray))
    assert(crcResult.get === correctCRC32Val)
  }

  "a HashExecutionMap" should "generate md5" in {
    val crcResult = HashUtils.HashExecutionMap.get("md5").map(f => f.apply(inputArray))
    assert(crcResult.get === correctMD5Val)
  }
  "a HashExecutionMap" should "generate sha1" in {
    val crcResult = HashUtils.HashExecutionMap.get("sha1").map(f => f.apply(inputArray))
    assert(crcResult.get === correctSHA1Val)
  }
  "a HashExecutionMap" should "generate sha224" in {
    val crcResult = HashUtils.HashExecutionMap.get("sha224").map(f => f.apply(inputArray))
    assert(crcResult.get === correctSHA224Val)
  }
  "a HashExecutionMap" should "generate sha256" in {
    val crcResult = HashUtils.HashExecutionMap.get("sha256").map(f => f.apply(inputArray))
    assert(crcResult.get === correctSHA256Val)
  }
  "a HashExecutionMap" should "generate sha384" in {
    val crcResult = HashUtils.HashExecutionMap.get("sha384").map(f => f.apply(inputArray))
    assert(crcResult.get === correctSHA384Val)
  }
  "a HashExecutionMap" should "generate sha512" in {
    val crcResult = HashUtils.HashExecutionMap.get("sha512").map(f => f.apply(inputArray))
    assert(crcResult.get === correctSHA512Val)
  }

}

object HashUtilsSpec {
  val inputArray = new String("123").getBytes
  val correctCRC32Val = "884863d2"
  val correctMD5Val = "202cb962ac59075b964b07152d234b70"
  val correctSHA1Val = "40bd001563085fc35165329ea1ff5c5ecbdbbeef"
  val correctSHA224Val = "78d8045d684abd2eece923758f3cd781489df3a48e1278982466017f"
  val correctSHA256Val = "a665a45920422f9d417e4867efdc4fb8a04a1f3fff1fa07e998e86f7f7a27ae3"
  val correctSHA384Val = "9a0a82f0c0cf31470d7affede3406cc9aa8410671520b727044eda15b4c25532a9b5cd8aaf9cec4919d76255b6bfb00f"
  val correctSHA512Val = "3c9909afec25354d551dae21590bb26e38d53f2173b8d3dc3eee4c047e7ab1c1eb8b85103e3be7" +
    "ba613b31bb5c9c36214dc9f14a42fd7a2fdb84856bca5c44c2"
}