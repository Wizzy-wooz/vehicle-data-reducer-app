package org.vodzianova
package utils

trait MandatorySchema {
  val vehicleId = "vehicle_id"
  val sampleTime = "sample_time"
  val defaultConfig = Map("vehicle_id" -> "vehicle_id", "sample_time" -> "time")
  val mandatoryColumns: Seq[String] = Seq(vehicleId, sampleTime)
}
