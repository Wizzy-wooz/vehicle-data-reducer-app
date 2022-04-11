package org.vodzianova
package utils

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * Setting internal Spark parameters.
 * Setting the configuration parameters listed below correctly is very important
 * and determines the source consumption and performance of Spark basically.
 * This is just an example. Production ready parameters must be adjusted.
 *
 */
trait SparkSessionWrapper {
  val conf: SparkConf = new SparkConf()
    .setAppName("Vehicle Data Reducer App")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .set("spark.driver.memory", "55g")
    .set("driver-memory", "48g")
    .set("spark.driver.maxResultSize", "8g")

  lazy val sparkSession: SparkSession =
    SparkSession
      .builder()
      .config(conf)
      .getOrCreate()
}
