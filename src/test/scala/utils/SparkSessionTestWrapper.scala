package org.vodzianova
package utils

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

trait SparkSessionTestWrapper {
  val conf: SparkConf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("Vehicle Data Reducer App Test")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .config(conf)
      .getOrCreate()
  }
}