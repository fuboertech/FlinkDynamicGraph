package org.ilmenau.groupstudy.flinkdynamicgraph.graph

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.graph.scala.Graph
import org.apache.flink.graph.{Edge, Vertex}
import org.apache.flink.streaming.api.scala._
import org.ilmenau.groupstudy.flinkdynamicgraph.loader.DataLoader
import org.ilmenau.groupstudy.flinkdynamicgraph.model.Route

class AirlinesGraph(env: ExecutionEnvironment) extends AbstractGraph(env: ExecutionEnvironment) {

  def construct(): Unit = {
    // key: airport id, value: Airport object
    val vertices = DataLoader.airports.map(a => new Vertex(a.airportID, a))

    // val routesWithAirlines = DataLoader.routes.join(DataLoader.airlines).where(1).equalTo(0)

    // from: source airport id, to: dest airport id, value: airlineID
    val edges = DataLoader.routes.map(j => new Edge(j.sourceAirportID, j.destAirportID, j.airlineID))

    graph = Graph.fromDataSet(vertices, edges, env)
  }

  override def addEdges(routes: Iterable[Route]): Unit = {
    val edges = env.fromCollection(routes)
      .map(j => new Edge(j.sourceAirportID, j.destAirportID, j.airlineID))
    graph = graph.addEdges(edges.collect().toList)
    println("Graph edges: " + graph.getEdges.count() + "\n")
  }

}
