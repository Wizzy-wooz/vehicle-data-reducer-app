package org.example
package model

case class VehicleData(vehicle_id: String, sample_time: Long, speed: Double, fuel_level: Double, rank: Integer)
case class VehicleDataTransformed(vehicle_id: String, sample_time: Long,  rank: Integer, speed: Double, fuel_level: Double)
