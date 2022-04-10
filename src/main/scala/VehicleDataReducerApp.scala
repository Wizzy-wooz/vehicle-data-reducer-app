package org.example

import jobs.VehicleDataJob
import utils.ArgsParser.parseArguments

import org.kohsuke.args4j.Option

class VehicleDataReducerApp(args: Array[String]) {

  @Option(name="-c", aliases=Array("--config-path"), usage="Path to schema config file", required = true)
  var configPath: String = "src/main/resources/schema_conf.json"

  @Option(name="-s", aliases=Array("--data-source-path"), usage="Path to source file", required = true)
  var sourcePath: String = "src/main/resources/vehicle_data.csv"

  @Option(name="-t", aliases=Array("--output-path"), usage="Path to output file", required = true)
  var targetPath: String = "src/main/resources/result/vehicle_data_reduced.json"

  @Option(name="-w", aliases=Array("--time-window"), usage="Time window. Default 10 seconds.", required = true)
  var windowDuration: String = "10 seconds"

  def run(): Unit = {
    parseArguments(args)
    VehicleDataJob.reduce(configPath, sourcePath, targetPath, windowDuration)
  }
}

object VehicleDataReducerApp {
  def main(args: Array[String]): Unit = {
    new VehicleDataReducerApp(args).run()
  }
}
