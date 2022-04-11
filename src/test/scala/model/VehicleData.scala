package org.vodzianova
package model

case class VehicleData(vehicle_id: String, sample_time: Long, speed: Double, fuel_level: Double, rank: Int)
case class VehicleDataTransformed(vehicle_id: String, sample_time: Long,  rank: Int, speed: Double, fuel_level: Double)
