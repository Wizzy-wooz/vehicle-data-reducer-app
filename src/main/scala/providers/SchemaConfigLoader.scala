package org.vodzianova
package providers

/**
 * Implemented only for json config files now.
 *
 */
object SchemaConfigLoader extends SchemaConfigLoadProvider {

  override def loadConfig(path: String): Map[String, String] = {
    val config = path match {
      case x if x.endsWith(".json") =>
        sparkSession
          .read
          .option("multiline", "true")
          .json(path)
          .na
          .drop()
          .collect()
          .map(row => row.getAs[String](0) -> row.getAs[String](1))
          .toMap
      case _ => throw new RuntimeException(s"Can't load config $path as file format is not supported yet.")
    }

    validateConfig(path, config)
  }
}
