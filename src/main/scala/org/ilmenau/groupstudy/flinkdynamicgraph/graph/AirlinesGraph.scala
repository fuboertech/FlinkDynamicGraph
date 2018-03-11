package org.ilmenau.groupstudy.flinkdynamicgraph.graph

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.graph.scala.Graph
import org.apache.flink.graph.scala._
import org.apache.flink.graph.{Edge, Vertex}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.types.DoubleValue
import org.ilmenau.groupstudy.flinkdynamicgraph.algorithms.PageRankAlgorithm
import org.ilmenau.groupstudy.flinkdynamicgraph.algorithms.ShortestPathAlgorithm
import org.ilmenau.groupstudy.flinkdynamicgraph.loader.DataLoader
import org.ilmenau.groupstudy.flinkdynamicgraph.model.data.Route

class AirlinesGraph(env: ExecutionEnvironment) extends AbstractGraph(env: ExecutionEnvironment) {

  private var _fullPageRank: Seq[(Integer, DoubleValue)] = _
  private var _fullShortestPath: Seq[(Integer, DoubleValue)] = _

  def construct(): Unit = {

    // key: airport id, value: Airport object
    val vertices = DataLoader.airports.map(a => new Vertex(a.airportID, Double.PositiveInfinity))

    // val routesWithAirlines = DataLoader.routes.join(DataLoader.airlines).where(1).equalTo(0)

    // from: source airport id, to: dest airport id, value: airlineID
    val edges = DataLoader.routes.map(j => new Edge(j.sourceAirportID, j.destAirportID, j.airlineID))
    graph = Graph.fromDataSet[Integer, Double, Integer](vertices, edges, env)

    graph.getVertices.print()

//    _fullPageRank = PageRankAlgorithm.runClassic(graph)
    _fullShortestPath = ShortestPathAlgorithm.run(graph)
  }

  override def addEdges(routes: Iterable[Route]): Unit = {
//    val edges = env.fromCollection(routes)
//      .map(j => new Edge(j.sourceAirportID, j.destAirportID, j.stops))
//    val addedEdges = edges.collect()
//    graph = graph.addEdges(addedEdges.toList)
//    println("Graph edges: " + graph.getEdges.count() + "\n")
//
//    val dynamicPageRank = PageRankAlgorithm.runDynamic(graph, addedEdges, _fullPageRank).toSeq
//    val classicPageRnnk = PageRankAlgorithm.runClassic(graph)
//    println("Count dynamic: "+ dynamicPageRank.size + "; classic: " + classicPageRnnk.size)

    val fullShortestPath = ShortestPathAlgorithm.run(graph)

//    env.fromCollection(tuples2).leftOuterJoin(DataLoader.airports).where(0).equalTo(0) {
//      (airportIdWithPageRankValue, airport) =>
//        val pageRank:DoubleValue = if (airportIdWithPageRankValue == null) new DoubleValue() else airportIdWithPageRankValue._2
//        val r:String = if (airport == null) " none " else airport.name + " " + airport.city + " " + airport.country
//        val numOfEdgesToAirport:Int = edges.count(f => f.getTarget == airportIdWithPageRankValue._1)
//        Tuple3[DoubleValue, String, Int](pageRank, r, numOfEdgesToAirport)
//    }.sortPartition(0, Order.ASCENDING).setParallelism(1).print()

//    env.fromCollection(dynamicPageRank).join(env.fromCollection(classicPageRnnk)).where(0).equalTo(0) {
//      (dynamic, classic) =>
//        if (!dynamic._2.equals(classic._2)) {
//          println(dynamic +"] != [" + classic)
//        }
//        (dynamic._1, dynamic._2, classic._2)
//    }.collect()
  }

}
