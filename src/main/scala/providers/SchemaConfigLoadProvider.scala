package org.example
package providers

import utils.{MandatorySchema, SparkSessionWrapper}

trait SchemaConfigLoadProvider extends SparkSessionWrapper with MandatorySchema{

  def loadConfig(path: String): Map[String, String] = {
    validateConfig(path, defaultConfig)
  }

  def validateConfig(path: String, config: Map[String, String]): Map[String, String] = {
    if (mandatoryColumns.forall(config.keySet.contains(_))) config
    else throw new RuntimeException(s"Illegal config: file $path doesn't contain mandatory fields: ${mandatoryColumns.toString()}")
  }
}
