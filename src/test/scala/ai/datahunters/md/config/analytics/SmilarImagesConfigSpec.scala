package ai.datahunters.md.config.analytics

import ai.datahunters.md.{TagValueType, UnitSpec}
import com.typesafe.config.ConfigFactory

import scala.collection.JavaConversions.mapAsJavaMap

class SmilarImagesConfigSpec extends UnitSpec {

  "A SimilarImagesConfig" should "load default config" in {
    val inputConfig = Map(
      SimilarImagesConfig.TagsInfoKey -> "GPS.GPS Altitude Ref:STRING,Exif IFD0.Model:STRING:1"
    )
    val config = ConfigFactory.parseMap(inputConfig)
    val similarImagesConfig = SimilarImagesConfig.build(config)
    assert(similarImagesConfig.minPassingConditions === 2)
    assert(similarImagesConfig.tags.size === 2)
    val tag1Info = similarImagesConfig.tags(0)
    assert(tag1Info.valueType === TagValueType.STRING)
    assert(tag1Info.maxDifference === 0)
    assert(tag1Info.directory === "GPS")
    assert(tag1Info.tag === "GPS Altitude Ref")
    val tag2Info = similarImagesConfig.tags(1)
    assert(tag2Info.valueType === TagValueType.STRING)
    assert(tag2Info.maxDifference === 1)
    assert(tag2Info.directory === "Exif IFD0")
    assert(tag2Info.tag === "Model")
  }

  it should "load config overriding defaults" in {
    val inputConfig = Map(
      SimilarImagesConfig.TagsInfoKey -> "GPS.GPS Altitude Ref:STRING,Custom Dir1.Custom Tag1:INT:1,Custom Dir1.Custom Tag2:FLOAT:5,Custom Dir1.Custom Tag3:LONG:3",
      SimilarImagesConfig.MinPassingConditionsKey -> 1
    )
    val config = ConfigFactory.parseMap(inputConfig)
    val similarImagesConfig = SimilarImagesConfig.build(config)
    assert(similarImagesConfig.minPassingConditions === 1)
    assert(similarImagesConfig.tags.size === 4)
    val tag1Info = similarImagesConfig.tags(0)
    assert(tag1Info.valueType === TagValueType.STRING)
    assert(tag1Info.maxDifference === 0)
    assert(tag1Info.directory === "GPS")
    assert(tag1Info.tag === "GPS Altitude Ref")
    val tag2Info = similarImagesConfig.tags(1)
    assert(tag2Info.valueType === TagValueType.INT)
    assert(tag2Info.maxDifference === 1)
    assert(tag2Info.directory === "Custom Dir1")
    assert(tag2Info.tag === "Custom Tag1")
    val tag3Info = similarImagesConfig.tags(2)
    assert(tag3Info.valueType === TagValueType.FLOAT)
    assert(tag3Info.maxDifference === 5)
    assert(tag3Info.directory === "Custom Dir1")
    assert(tag3Info.tag === "Custom Tag2")
    val tag4Info = similarImagesConfig.tags(3)
    assert(tag4Info.valueType === TagValueType.LONG)
    assert(tag4Info.maxDifference === 3)
    assert(tag4Info.directory === "Custom Dir1")
    assert(tag4Info.tag === "Custom Tag3")
  }

  it should "throw exception when there is no required configuration" in {

  }

}
