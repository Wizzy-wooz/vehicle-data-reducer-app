package org.example
package utils

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

trait SparkSessionWrapper {
  val conf: SparkConf = new SparkConf()
    .setAppName(getClass.getSimpleName)
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

  lazy val sparkSession: SparkSession =
    SparkSession
      .builder()
      .config(conf)
      .getOrCreate()
}
