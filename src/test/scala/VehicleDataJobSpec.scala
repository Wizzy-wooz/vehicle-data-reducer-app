package org.example

import jobs.VehicleDataJob
import jobs.VehicleDataJob.sparkSession
import model.{VehicleData, VehicleDataTransformed}
import utils.SparkSessionTestWrapper

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class VehicleDataJobSpec extends AnyFlatSpec with SparkSessionTestWrapper with Matchers with DataFrameComparer {

  import spark.implicits._

  val testDataDF: DataFrame = Seq(
    VehicleData("AAA", 1500000001, 51.3, 54.2, 1),
    VehicleData("AAA", 1500000005, 54.7, 54.2, 2),
    VehicleData("BBB", 1500000005, 23.5, 97.5, 1),
    VehicleData("AAA", 1500000007, 53.1, 54.1, 3),
    VehicleData("BBB", 1500000008, 22.2, 97.5, 2),
    VehicleData("AAA", 1500000012, 54.3, 54.1, 4)
  ).toDF

  "VehicleDataJob" should "keep only the last sample for every vehicle in every 10 seconds window." in {
    val actualDF = VehicleDataJob.findLatestVehicleIdSampleInTimeWindow(testDataDF, "10 seconds")

    val expectedDF = Seq(
      VehicleDataTransformed("AAA", 1500000007, 3, 53.1, 54.1),
      VehicleDataTransformed("BBB", 1500000008, 2, 22.2, 97.5),
      VehicleDataTransformed("AAA", 1500000012, 4, 54.3, 54.1)
    ).toDF

    assertSmallDataFrameEquality(actualDF, expectedDF)
  }

  //TODO make as integration test
  "it" should "reduce vehicle data input dataset to expected output dataset according to specified schema config and time window." in {
    val targetPath = "src/test/resources/result/vehicle_data_reduced.json"

    VehicleDataJob.reduce(
      "src/test/resources/schema_conf.json",
      "src/test/resources/vehicle_data.csv",
      "src/test/resources/result/vehicle_data_reduced.json",
      "10 seconds"
    )

    val actualData: Seq[Row] =
      sparkSession
        .read
        .json(targetPath)
        .collect()

    val expectedData = Seq(
      Row(1580558406, "4T1BF28B62U270690", 37.0),
      Row(1580558417, "4T1BF28B62U270690", 50.7),
      Row(1580558428, "4T1BF28B62U270690", 43.1),
      Row(1580558409, "1FAFP34P32W126938", 41.1),
      Row(1580558414, "1FAFP34P32W126938", 40.8),
      Row(1580558427, "1FAFP34P32W126938", 42.1),
      Row(1580558436, "1FAFP34P32W126938", 44.4)
    )

    actualData should be eq expectedData
  }
}





