package org.ilmenau.groupstudy.flinkdynamicgraph.loader

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.streaming.api.scala._
import org.ilmenau.groupstudy.flinkdynamicgraph.model.{Airline, Airport, Route}

object DataLoader {

  private var _routes: DataSet[Route] = _

  private var _airlines: DataSet[Airline] = _

  private var _airports: DataSet[Airport] = _

  def load(e: ExecutionEnvironment): Unit = {
    val env = e
    _routes = env.readCsvFile[Route](
      getClass.getResource("/routes.dat").getPath.replace("!", ""))
    _airlines = env.readCsvFile[Airline](
      getClass.getResource("/airlines.dat").getPath.replace("!", ""),
      quoteCharacter = '\"')
    _airports = env.readCsvFile[Airport](
      getClass.getResource("/airports.dat").getPath.replace("!", ""),
      //lenient=true,
      quoteCharacter = '\"', includedFields = Array(0,1,2,3,4,5,6,7,8,9,10,11,12))
  }

  def routes: DataSet[Route] = _routes

  def airlines: DataSet[Airline] = _airlines

  def airports: DataSet[Airport] = _airports

  def airport(airportId: Int): Airport = _airports.filter(a => a.airportID == airportId).collect().head

}
