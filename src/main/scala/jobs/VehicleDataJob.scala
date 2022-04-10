package org.example
package jobs

import providers.SchemaConfigLoader.loadConfig
import utils.{MandatorySchema, SparkSessionWrapper}

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SaveMode}

object VehicleDataJob extends SparkSessionWrapper with MandatorySchema {
  def reduce(configPath: String, sourcePath: String, targetPath: String, windowDuration: String): Unit = {
    val vehicleDataDF =
      sparkSession
        .read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(sourcePath)
        .withColumn("rank", row_number().over(Window.partitionBy(vehicleId).orderBy(sampleTime)))

    val transformedVehicleDataDF = findLatestVehicleIdSampleInTimeWindow(vehicleDataDF, windowDuration)

    val cols = vehicleDataDF.columns.collect {
      name =>
        loadConfig(configPath).get(name)
        match {
          case Some(newname) => col(name).as(newname): Column
        }
    }

    transformedVehicleDataDF.select(cols: _*)
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .json(targetPath)
  }

  def findLatestVehicleIdSampleInTimeWindow(vehicleDataDF: DataFrame, windowDuration: String): DataFrame = {
    vehicleDataDF.join(broadcast(vehicleDataDF
      .withColumn("ts_sample_time", from_unixtime(col(sampleTime)))
      .groupBy(col(vehicleId), window(col("ts_sample_time"), windowDuration))
      .max(sampleTime, "rank")
      .withColumnRenamed("max(sample_time)", sampleTime)
      .withColumnRenamed("max(rank)", "rank")
      .drop("window")), mandatoryColumns :+ "rank")
  }
}
