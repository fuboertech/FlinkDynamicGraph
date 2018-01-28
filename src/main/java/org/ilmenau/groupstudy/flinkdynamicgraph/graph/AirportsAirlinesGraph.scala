package org.ilmenau.groupstudy.flinkdynamicgraph.graph

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.graph.{Edge, Vertex}
import org.apache.flink.graph.scala.Graph
import org.apache.flink.streaming.api.scala._
import org.ilmenau.groupstudy.flinkdynamicgraph.loader.DataLoader
import org.ilmenau.groupstudy.flinkdynamicgraph.model.{Airline, Airport, Route}

object AirportsAirlinesGraph {

  private var _graph: Graph[Integer, Airport, (Route, Airline)] = _

  def construct(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    // load databases
    DataLoader.load()

    // key: airport id, value: Airport object
    val vertices = DataLoader.airports.map(a => new Vertex(a.airportID, a))

    var routesWithAirlines = DataLoader.routes.join(DataLoader.airlines).where(1).equalTo(0)

    // from: source airport id, to: dest airport id, value: result of Routes and Airlines joined by airlineID
    val edges = routesWithAirlines.map(j => new Edge(j._1.sourceAirportID, j._1.destAirportID, j))

    _graph = Graph.fromDataSet(vertices, edges, env)
  }

  def get: Graph[Integer, Airport, (Route, Airline)] = _graph
}
