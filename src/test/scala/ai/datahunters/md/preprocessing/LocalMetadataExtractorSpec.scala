package ai.datahunters.md.preprocessing

import ai.datahunters.md.UnitSpec

class LocalMetadataExtractorSpec extends UnitSpec {

  "A LocalMetadataExtractor" should "extract metadata from local file system" in {
    val md = LocalMetadataExtractor.extract(imgPath("landscape-4518195_960_720_pixabay_license.jpg"))
    assert(md.tagsCount === 19)
    assert(md.dirs.sorted === Seq("Exif IFD0", "File Type", "Huffman", "JFIF", "JPEG"))
  }

}
