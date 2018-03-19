package org.ilmenau.groupstudy.flinkdynamicgraph.loader

import java.io._

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.streaming.api.scala._
import org.ilmenau.groupstudy.flinkdynamicgraph.model.data.{Airline, Airport, Route}

object DataLoader {

  private var _routes: DataSet[Route] = _

  private var _airlines: DataSet[Airline] = _

  private var _airports: DataSet[Airport] = _

  private var _testGraphEdges: DataSet[Route] = _

  def load(env: ExecutionEnvironment): Unit = {
    _routes = env.readCsvFile[Route](
      getPath("/routes.dat"))
    _airlines = env.readCsvFile[Airline](
      getPath("/airlines.dat"),
      quoteCharacter = '\"')
    _airports = env.readCsvFile[Airport](
      getPath("/airports.dat"),
      //lenient=true,
      quoteCharacter = '\"', includedFields = 0 to 12 toArray)

    _testGraphEdges = env.readCsvFile[Route](
      getPath("/test_graph_edges.dat"))
  }

  def routes: DataSet[Route] = _routes.filter(r => r.destAirportID != -1 && r.sourceAirportID != -1)

  def airlines: DataSet[Airline] = _airlines

  def airports: DataSet[Airport] = _airports

  def airport(airportId: Int): Airport = _airports.filter(a => a.airportID == airportId).collect().head

  def testGraphEdtes: DataSet[Route] = _testGraphEdges

  private def getPath(resourceName: String): String =  {
    var path = ""
    try {
      val input = getClass.getResourceAsStream(resourceName)

      val file = File.createTempFile(resourceName, ".tmp")

      val out: OutputStream = new FileOutputStream(file)
      var read: Int = 0
      val bytes: Array[Byte] = new Array[Byte](1024)
      while ({ read = input.read(bytes); read } != -1) {
        out.write(bytes, 0, read)
      }

      file.deleteOnExit()
      path = file.getPath
    } catch {
      case e: Exception => e.printStackTrace()
    }
    path
  }

}
