package org.vodzianova
package jobs

import providers.SchemaConfigLoader.loadConfig
import utils.{MandatorySchema, SparkSessionWrapper}

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SaveMode}

/**
 * Spark Job uses a configuration file that map between the input file fields to the required output fields.
 * Each original field will be assigned an output name. if no name is indicated, that means the field should not be part of the output.
 * Also it makes sure every vehicle have no more than one sample in every 10 seconds window. If there are more samples than that it will keep only the last one.
 */
object VehicleDataJob extends SparkSessionWrapper with MandatorySchema {
  def reduce(configPath: String, sourcePath: String, targetPath: String, windowDuration: String, parallelism: Int): Unit = {
    val vehicleDataDF =
      sparkSession
        .read
        .option("header", "true")
        .option("inferSchema", "true")
        .option("mode", "FAILFAST")
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
      .repartition(parallelism)
      .write
      .mode(SaveMode.Overwrite)
      .json(targetPath)
  }

  def findLatestVehicleIdSampleInTimeWindow(vehicleDataDF: DataFrame, windowDuration: String): DataFrame = {
    val latestSampleVehicleDataDF = vehicleDataDF
      .withColumn("ts_sample_time", from_unixtime(col(sampleTime)))
      .groupBy(col(vehicleId), window(col("ts_sample_time"), windowDuration))
      .max(sampleTime, "rank")
      .withColumnRenamed("max(sample_time)", sampleTime)
      .withColumnRenamed("max(rank)", "rank")
      .drop("window")

    vehicleDataDF.join(broadcast(latestSampleVehicleDataDF), mandatoryColumns :+ "rank")
  }
}
