package ai.datahunters.md.udf.hash

import java.nio.file.{Files, Paths}

import ai.datahunters.md.UnitSpec
import ai.datahunters.md.udf.ExtractorsSpec.ImagePath

class HashExtractorSpec extends UnitSpec {

  import HashExtractor._

  "generateHashFromContent" should "generate CRC32 hash" in {
    val file = Files.readAllBytes(Paths.get(imgPath(ImagePath)))
    val result = generateHashFromContent("crc32")(file)
    assert(result === Option("d11d03c0"))
  }

  "generateHashFromContent" should "equal none" in {
    val file = Files.readAllBytes(Paths.get(imgPath(ImagePath)))
    val result = generateHashFromContent("crc-32")(file)
    assert(result === None)
  }
}
