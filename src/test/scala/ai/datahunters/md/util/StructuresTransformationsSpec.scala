package ai.datahunters.md.util

import ai.datahunters.md.UnitSpec

class StructuresTransformationsSpec extends UnitSpec {

  import StructuresTransformationsSpec._

  "A reverseNestedMap" should "return flat map with reversed keys" in {

    val resultMap = StructuresTransformations.reverseNestedMap(SampleInputMap)
    val expectedMap = Map(
      "embeddedK1" -> "mainK1",
      "embeddedK2" -> "mainK2",
      "embeddedK3" -> "mainK2"
    )
    assert(resultMap === expectedMap)
  }

  "A concatKeys" should "produce flat map where keys are concatenation of root key and nested" in {
    val resultMap = StructuresTransformations.concatKeys(SampleInputMap)
    val expectedMap = Map(
      "mainK1 embeddedK1" -> "val1",
      "mainK1 embeddedK2" -> "val2",
      "mainK2 embeddedK3" -> "val3",
      "mainK2 embeddedK2" -> "val4"
    )
    assert(resultMap === expectedMap)
  }

  "A concatKeysToSeq" should "produce list where elements are concatenation of root key and nested" in {
    val result = StructuresTransformations.concatKeysToSeq(SampleInputMap)
    val expected = Seq(
      "mainK1 embeddedK1",
      "mainK1 embeddedK2",
      "mainK2 embeddedK3",
      "mainK2 embeddedK2"
    )
    assert(result === expected)
  }

  "A concatKeysToSeqIfValueNotNull" should "produce list where elements are concatenation of root key and nested" +
    " which value is not empty" in {
    val result = StructuresTransformations.concatKeysToSeqIfValueNotNull(SampleInputMapWithEmptyValue)
    val expected = Seq(
      "mainK1.embeddedK1",
      "mainK2.embeddedK3"
    )
    assert(result === expected)
  }
}

object StructuresTransformationsSpec {

  val SampleInputMap = Map(
    "mainK1" -> Map(
      "embeddedK1" -> "val1",
      "embeddedK2" -> "val2"
    ),
    "mainK2" -> Map(
      "embeddedK3" -> "val3",
      "embeddedK2" -> "val4"
    )
  )

  val SampleInputMapWithEmptyValue = Map(
    "mainK1" -> Map(
      "embeddedK1" -> "val1",
      "embeddedK2" -> ""
    ),
    "mainK2" -> Map(
      "embeddedK3" -> "val3",
      "embeddedK2" -> null
    )
  )
}