package org.ilmenau.groupstudy.flinkdynamicgraph.loader

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.streaming.api.scala._
import org.ilmenau.groupstudy.flinkdynamicgraph.model.{Airlines, Airports, Routes}

object DataLoader {

  private var _routes: DataSet[Routes] = _

  private var _airlines: DataSet[Airlines] = _

  private var _airports: DataSet[Airports] = _

  def load(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    _routes = env.readCsvFile[Routes](
      getClass.getResource("/routes.dat").getPath)
    _airlines = env.readCsvFile[Airlines](
      getClass.getResource("/airlines.dat").getPath,
      quoteCharacter = '\"')
    _airports = env.readCsvFile[Airports](
      getClass.getResource("/airports.dat").getPath,
      //lenient=true,
      quoteCharacter = '\"', includedFields = Array(0,1,2,3,4,5,6,7,8,9,10,11,12))
  }

  def routes: DataSet[Routes] = _routes

  def airlines: DataSet[Airlines] = _airlines

  def airports: DataSet[Airports] = _airports

}
