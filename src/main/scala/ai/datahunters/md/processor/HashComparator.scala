package ai.datahunters.md.processor


import org.apache.spark.sql.DataFrame

case class HashComparator(solrHashDF: Option[DataFrame],
                          hashList: Seq[String],
                          nameConverter: ColumnNamesConverter) extends Processor {

  import ai.datahunters.md.processor.HashComparator._
  import ai.datahunters.md.processor.HashExtractor._

  val hashColumns: Seq[String] = hashList.map(s => HashColPrefix + s)
    .map(s => nameConverter.namingConvention(s))


  override def execute(inputDF: DataFrame): DataFrame = {
    if (hashColumns.nonEmpty) {
      solrHashDF.map(df => {
        inputDF.join(
          df,
          hashColumns,
          JoinType
        )
      })
        .getOrElse(inputDF)
    } else {
      inputDF
    }
  }
}

object HashComparator {

  val JoinType = "left_anti"
}
