package ai.datahunters.md.processor

import ai.datahunters.md.util.TextUtils

object ColumnNamesConverterFactory {

  def create(namingConvention: String): ColumnNamesConverter = ColumnNamesConverter(
    TextUtils.NamingConvention(namingConvention)
  )

}
