package ai.datahunters.md.processor


import org.apache.spark.sql.DataFrame

case class HashComparator(solrHashDF: Option[DataFrame],
                          hashList: Seq[String]) extends Processor {

  import ai.datahunters.md.processor.HashComparator._

  val HashColumns : Seq[String] = hashList.map(s => HashPrefix + s)


  override def execute(inputDF: DataFrame): DataFrame = {
    solrHashDF.map(df => {
      inputDF.join(
        df,
        HashColumns,
        JoinType
      )
    })
      .getOrElse(inputDF)
  }
}

object HashComparator {

  val JoinType = "left_anti"
  val HashPrefix = "hash_"
}
