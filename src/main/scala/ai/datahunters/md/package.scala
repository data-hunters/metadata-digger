package ai.datahunters

import ai.datahunters.md.TagValueType.TagValueType
import ai.datahunters.md.util.{Parser, ParserOpt, TextUtils}

package object md {

  case class MetatagSimilarityDefinition(directory: String, tag: String, valueType: TagValueType, maxDifference: BigDecimal)

  case class MetatagSimilarity[T](definition: MetatagSimilarityDefinition, value: T) {}

  object MetatagSimilarity {

    def create[T](definition: MetatagSimilarityDefinition, value: String) = {
      definition.valueType match {
        case TagValueType.STRING => MetatagSimilarity(definition, value)
        case TagValueType.INT => MetatagSimilarity(definition, Parser.parse[Int](value))
        case TagValueType.LONG => MetatagSimilarity(definition, Parser.parse[Long](value))
        case TagValueType.FLOAT => MetatagSimilarity(definition, Parser.parse[Float](value))
        case _ => throw new RuntimeException(s"Not Supported type of tag: ${definition.valueType}")
      }
    }

  }

  case class MetadataInfo(tags: Map[String, Map[String, String]], dirNames: Seq[String], tagNames: Seq[String], tagsCount: Int, fileType: String) {

    def show(): Unit = {
      println()
      println(s"Number of Metadata Directories: ${dirNames.size}")
      println(s"Number of Metadata Tags: ${tagsCount}")
      println()
      tags.foreach(dir => {
        println(s"${dir._1}:")
        dir._2.foreach(tag => {
          println(s"\t${tag._1}: ${tag._2}")
        })
        println("------------------------------------------------")
      })
    }
  }

  object TagValueType extends Enumeration {
    type TagValueType = Value
    val INT, LONG, FLOAT, STRING = Value

    def parse(valueType: Value, value: String): Any = valueType match {
      case TagValueType.STRING => value
      case TagValueType.INT => Parser.parse[Int](value)
      case TagValueType.LONG => Parser.parse[Long](value)
      case TagValueType.FLOAT => Parser.parse[Float](value)
      case _ => throw new RuntimeException(s"Not Supported type of tag: ${valueType}")
    }
  }

}
