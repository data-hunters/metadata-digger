package ai.datahunters.md.udf.analytics

import ai.datahunters.md.{MetatagSimilarity, MetatagSimilarityDefinition, TagValueType, UnitSpec}

class SimilaritiesSpec extends UnitSpec {

  import ai.datahunters.md.udf.analytics.Similarities.Transformations._
  import SimilaritiesSpec._

  "checkLevDistanceT" should "count edit distances between tags lower than thresholds" in {
    val similarities1 = MetatagSimilarity.create(MetatagSimilarityDefinition("dir1", "tag1", TagValueType.STRING, 2), "val12").asInstanceOf[MetatagSimilarity[String]]
    val similarities2 = MetatagSimilarity.create(MetatagSimilarityDefinition("dir2", "tag3", TagValueType.STRING, 0), "val33").asInstanceOf[MetatagSimilarity[String]]
    val distancesCount = checkLevDistanceT(Seq(similarities1, similarities2))(SampleStringTags)
    assert(distancesCount === 1)
  }

  "checkSimilarIntsT" should "count ints differences lower than thresholds" in {
    val similarities1 = MetatagSimilarity.create(MetatagSimilarityDefinition("dir1", "tag1", TagValueType.INT, 3), "7").asInstanceOf[MetatagSimilarity[Int]]
    val similarities2 = MetatagSimilarity.create(MetatagSimilarityDefinition("dir2", "tag3", TagValueType.INT, 1), "1").asInstanceOf[MetatagSimilarity[Int]]
    val diffCount = checkSimilarIntsT(Seq(similarities1, similarities2))(SampleIntsTags)
    assert(diffCount === 2)
  }

  "checkSimilarLongsT" should "count longs differences lower than thresholds" in {
    val similarities1 = MetatagSimilarity.create(MetatagSimilarityDefinition("dir1", "tag1", TagValueType.LONG, 3), (Long.MaxValue - 12L).toString).asInstanceOf[MetatagSimilarity[Long]]
    val similarities2 = MetatagSimilarity.create(MetatagSimilarityDefinition("dir2", "tag3", TagValueType.LONG, 1), "1").asInstanceOf[MetatagSimilarity[Long]]
    val diffCount = checkSimilarLongsT(Seq(similarities1, similarities2))(SampleLongsTags)
    assert(diffCount === 2)
  }

  "checkSimilarFloatsT" should "count floats differences lower than thresholds" in {
    val similarities1 = MetatagSimilarity.create(MetatagSimilarityDefinition("dir1", "tag1", TagValueType.FLOAT, 2.5), "45.2").asInstanceOf[MetatagSimilarity[Float]]
    val similarities2 = MetatagSimilarity.create(MetatagSimilarityDefinition("dir2", "tag3", TagValueType.FLOAT, 5.2), "12.1").asInstanceOf[MetatagSimilarity[Float]]
    val similarities3 = MetatagSimilarity.create(MetatagSimilarityDefinition("dir2", "tag4", TagValueType.FLOAT, 1.2), "5.4").asInstanceOf[MetatagSimilarity[Float]]
    val diffCount = checkSimilarFloatsT(Seq(similarities1, similarities2))(SampleFloatsTags)
    assert(diffCount === 2)
  }
}

object SimilaritiesSpec {

  val SampleStringTags = Map("dir1" -> Map("tag1" -> "val1"), "dir2" -> Map("tag3" -> "val3", "tag4" -> "val4"))
  val SampleIntsTags = Map("dir1" -> Map("tag1" -> "5"), "dir2" -> Map("tag3" -> "2", "tag4" -> "val4"))
  val SampleLongsTags = Map("dir1" -> Map("tag1" -> (Long.MaxValue - 10L).toString), "dir2" -> Map("tag3" -> "2", "tag4" -> "val4"))
  val SampleFloatsTags = Map("dir1" -> Map("tag1" -> "47.6"), "dir2" -> Map("tag3" -> "17.27", "tag4" -> "4.1"))
}