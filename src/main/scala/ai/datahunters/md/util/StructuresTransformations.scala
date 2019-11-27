package ai.datahunters.md.util

object StructuresTransformations {

  /**
    * Retrieve keys of all nested maps and make it main keys, setting original main keys as values.
    * Let's suppose we have the following input map:
    * mainKey1:
    *   nestedKey1: nestedVal1
    *   nestedKey2: nestedVal2
    * mainKey2:
    *   nestedKey3: nestedVal3
    * The following method will produce below map:
    * nestedKey1: mainKey1
    * nestedKey2: mainKey1
    * nestedKey3: mainKey2
    *
    * @param nestedMap
    * @tparam T
    * @tparam V
    * @tparam Z
    * @return
    */
  def reverseNestedMap[T, V, Z](nestedMap: Map[T, Map[V, Z]]): Map[V, T] = {
    nestedMap.map(tagPair => (tagPair._1 -> tagPair._2.keySet))
      .flatMap(tagPair => tagPair._2.map(tagName => (tagName -> tagPair._1)).toMap)
  }

  /**
    * Retrieve keys of all nested maps and build map where each key is concatenation of main key and nested key and value is nested value.
    * Let's suppose we have the following input map:
    * "mainKey1":
    *   "nestedKey1": nestedVal1
    *   "nestedKey2": nestedVal2
    * "mainKey2":
    *   "nestedKey3": nestedVal3
    * The following method will produce below map:
    * "mainKey1 nestedKey1": nestedVal1
    * "mainKey1 nestedKey2": nestedVal2
    * "mainKey2 nestedKey3": nestedVal3
    *
    * @param nestedMap
    * @tparam T
    * @tparam V
    * @tparam Z
    * @return
    */
  def concatKeys[T](nestedMap: Map[String, Map[String, T]], delimiter: String = " "): Map[String, T] = {
    nestedMap.flatMap(tagPair => {
      tagPair._2
        .map(nestedPair => (s"${tagPair._1}${delimiter}${nestedPair._1}" -> nestedPair._2))
    })
  }

  /**
    * Retrieve keys of all nested maps and build list where each element is concatenation of main key and nested key.
    * Let's suppose we have the following input map:
    * "mainKey1":
    *   "nestedKey1": nestedVal1
    *   "nestedKey2": nestedVal2
    * "mainKey2":
    *   "nestedKey3": nestedVal3
    * The following method will produce below list:
    * "mainKey1 nestedKey1"
    * "mainKey1 nestedKey2"
    * "mainKey2 nestedKey3"
    *
    * @param nestedMap
    * @param delimiter
    * @tparam T
    * @return
    */
  def concatKeysToSeq[T](nestedMap: Map[String, Map[String, T]], delimiter: String = " "): Seq[String] = {
    nestedMap.flatMap(tagPair => {
      tagPair._2
        .map(nestedPair => s"${tagPair._1}${delimiter}${nestedPair._1}")
    }).toSeq
  }

  /**
    * Retrieve keys of all nested maps and build list where each element with non empty value
    * is concatenation of main key and nested key with dot sign as delimiter.
    * Let's suppose we have the following input map:
    * "mainKey1":
    *   "nestedKey1": nestedVal1
    *   "nestedKey2": nestedVal2
    *   "nestedKey3": nestedVal3(null or empty string)
    * "mainKey2":
    *   "nestedKey3": nestedVal4
    * The following method will produce below list:
    * "mainKey1 nestedKey1"
    * "mainKey1 nestedKey2"
    * "mainKey2 nestedKey3"
    *
    * @param nestedMap
    * @param delimiter
    * @tparam T
    * @return
    */
  def concatKeysToSeqIfValueNotNull[T](nestedMap: Map[String, Map[String, T]], delimiter: String = "."): Seq[String] = {
    nestedMap.flatMap(tagPair => {
      tagPair._2
        .filter(nestedPair=> nestedPair._2 != null && nestedPair._2 != "")
        .map(nestedPair => {
          s"${tagPair._1}${delimiter}${nestedPair._1}"
        })
    }).toSeq
  }
}
