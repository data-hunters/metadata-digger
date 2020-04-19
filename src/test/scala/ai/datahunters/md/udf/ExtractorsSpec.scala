package ai.datahunters.md.udf

import java.nio.file.{Files, Paths}

import ai.datahunters.md.UnitSpec
import ai.datahunters.md.schema.MetadataSchemaConfig
import ai.datahunters.md.udf.Extractors.Transformations
import ai.datahunters.md.util.TextUtils
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
class ExtractorsSpec extends UnitSpec {
  import ExtractorsSpec._
  import Extractors.Transformations._

  "extractMetadataT" should "extract metadata from image file" in {
    val file = Files.readAllBytes(Paths.get(imgPath(ImagePath)))
    val md = extractMetadataT()(ImagePath, file)
    val tags: Map[String, Map[String, String]] = md.getAs(0)
    val dirs: Seq[String] = md.getAs(1)
    val tagNames: Seq[String] = md.getAs(2)
    val tagsCount: Int = md.getAs(3)
    val fileType: String = md.getAs(4)
    val expectedTagPair = ("Component 3", "Cr component: Quantization table 1, Sampling factors 1 horiz/1 vert")
    val jpegDir = tags.get("JPEG").get
    assert(jpegDir.get(expectedTagPair._1).get === expectedTagPair._2)
    assert(ExpectedDirs === dirs)
    assert(tagsCount === 19)
    assert(fileType === "JPEG")
  }

  "extractMetadataT" should "ignore unsupported format and set unknown type" in {
    val file = Files.readAllBytes(Paths.get(configPath("full_local.config.properties")))
    val md = extractMetadataT()("some/path", file)
    val tags: Map[String, Map[String, String]] = md.getAs(0)
    val dirs: Seq[String] = md.getAs(1)
    val tagNames: Seq[String] = md.getAs(2)
    val tagsCount: Int = md.getAs(3)
    val fileType: String = md.getAs(4)
    assert(dirs.isEmpty)
    assert(tags.isEmpty)
    assert(tagsCount === 0)
    assert(fileType === MetadataExtractor.UnknownType)

  }

  "selectMetadata" should "select metadata from embedded structure" in {
    val md = Transformations.selectMetadataT(Seq(Dir1Name, Dir2Name))(EmbeddedMetadataSample)
    val dir1: Map[String, String] = md.getAs(0)
    val dir2: Map[String, String] = md.getAs(1)

    assert(md.length === 2)
    assert(dir1 === EmbeddedMetadataSample.get(Dir1Name).get)
    assert(dir2 === EmbeddedMetadataSample.get(Dir2Name).get)
  }

  "selectMetadataT" should "select metadata from embedded structure for particular directories only" in {
    val md = Transformations.selectMetadataT(Seq(Dir1Name))(EmbeddedMetadataSample)
    val dir1: Map[String, String] = md.getAs(0)

    assert(md.length === 1)
    assert(dir1 === EmbeddedMetadataSample.get(Dir1Name).get)
  }

  "selectMetadataTagsT" should "select tags from embedded map to flat structure" in {
    val md = selectMetadataTagsT(Seq(Tag1Name, Tag2Name, Tag3Name))(EmbeddedMetadataSample)
    val tag1: String = md.getAs(0)
    val tag2: String = md.getAs(1)
    val tag3: String = md.getAs(2)

    assert(md.length === 3)
    assert(ExpectedTag1Val === tag1)
    assert(ExpectedTag2Val === tag2)
    assert(ExpectedTag3Val === tag3)
  }

  "selectMetadataTagsT" should "select tags from embedded map to flat structure for particular tags" in {
    val md = selectMetadataTagsT(Seq(Tag1Name, Tag2Name))(EmbeddedMetadataSample)
    val tag1: String = md.getAs(0)
    val tag2: String = md.getAs(1)

    assert(md.length === 2)
    assert(ExpectedTag1Val === tag1)
    assert(ExpectedTag2Val === tag2)
  }

  "selectMetadataTagNamesT" should "retrieve all tag names from directories maps" in {
    val tagNames: Seq[String] = selectMetadataTagNamesT(false)(DirsMetadataSample)
    assert(Seq(Tag1Name, Tag2Name, Tag3Name).sorted === tagNames.sorted)
  }

  "selectMetadataTagsFromDirsT" should "select tags from directories maps to flat structure" in {
    val md = selectMetadataTagsFromDirsT(false, Seq(Tag1Name, Tag2Name, Tag3Name))(DirsMetadataSample)
    val tag1: String = md.getAs(0)
    val tag2: String = md.getAs(1)
    val tag3: String = md.getAs(2)

    assert(md.length === 3)
    assert(ExpectedTag1Val === tag1)
    assert(ExpectedTag2Val === tag2)
    assert(ExpectedTag3Val === tag3)
  }

  "selectMetadataTagsFromDirsT" should "select tags from directories maps to flat structure with adding directory name" in {
    val md = selectMetadataTagsFromDirsT(true, Seq( Dir1Name + " " + Tag1Name,  Dir2Name + " " + Tag2Name,  Dir2Name + " " + Tag3Name))(DirsMetadataSample)
    val tag1: String = md.getAs(0)
    val tag2: String = md.getAs(1)
    val tag3: String = md.getAs(2)

    assert(md.length === 3)
    assert(ExpectedTag1Val === tag1)
    assert(ExpectedTag2Val === tag2)
    assert(ExpectedTag3Val === tag3)
  }

  "selectMetadataTagsFromDirsT" should "select tags from directories maps to flat structure for particular tags" in {
    val md = selectMetadataTagsFromDirsT(false, Seq(Tag1Name, Tag3Name))(DirsMetadataSample)
    val tag1: String = md.getAs(0)
    val tag3: String = md.getAs(1)

    assert(md.length === 2)
    assert(ExpectedTag1Val === tag1)
    assert(ExpectedTag3Val === tag3)
  }
}

object ExtractorsSpec {

  val ImagePath = "landscape-4518195_960_720_pixabay_license.jpg"

  val Dir1Name = "dir1"
  val Dir2Name = "dir2"
  val Tag1Name = "tag1"
  val Tag2Name = "tag3"
  val Tag3Name = "tag4"
  val ExpectedTag1Val = "val1"
  val ExpectedTag2Val = "val3"
  val ExpectedTag3Val = "val4"

  val ExpectedDirs = Seq(
    "JPEG",
    "JFIF",
    "Exif IFD0",
    "Huffman",
    "File Type"
  )

  val EmbeddedMetadataSample = Map("dir1" -> Map("tag1" -> "val1"), "dir2" -> Map("tag3" -> "val3", "tag4" -> "val4"))

  val DirsMetadataSample = new GenericRowWithSchema(
    Array(Map("tag1" -> "val1"), Map("tag3" -> "val3", "tag4" -> "val4")),
    MetadataSchemaConfig(Seq("dir1", "dir2")).schema()
  )

}