package org.ilmenau.groupstudy.flinkdynamicgraph.graph

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.graph.scala.Graph
import org.apache.flink.graph.utils.GraphUtils.IdentityMapper
import org.apache.flink.graph.{Edge, Vertex}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.types.{DoubleValue, NullValue}
import org.ilmenau.groupstudy.flinkdynamicgraph.algorithms.PageRankAlgorithm
import org.ilmenau.groupstudy.flinkdynamicgraph.loader.DataLoader
import org.ilmenau.groupstudy.flinkdynamicgraph.model.data.Route

class AirlinesGraph(env: ExecutionEnvironment) extends AbstractGraph(env: ExecutionEnvironment) {

  private var _fullPageRank: Seq[(Integer, DoubleValue)] = _

  def construct(): Unit = {
    // key: airport id, value: Airport object
    val vertices = DataLoader.airports.map(a => new Vertex(a.airportID, new Integer(0)))

    // val routesWithAirlines = DataLoader.routes.join(DataLoader.airlines).where(1).equalTo(0)

    val start = 1
    val end   = 5
    val rnd = new scala.util.Random
    // from: source airport id, to: dest airport id, value: airlineID

    val edges = DataLoader.routes.map(j => new Edge(j.sourceAirportID, j.destAirportID, new Integer(start + rnd.nextInt( (end - start) + 1 )) ))


    //    graph = Graph.fromDataSet(edges, new IdentityMapper[Integer](), env)
//    graph = Graph.fromDataSet(edges, new IdentityMapper[Integer](), env)
    graph = Graph.fromDataSet[Integer, Integer, Integer](vertices, edges, env)
  }

//  private def genRandomStopsNumber(): Integer = {
//    val start = 1
//    val end   = 5
//    val rnd = new scala.util.Random
//    new Integer(start + rnd.nextInt( (end - start) + 1 ))
//  }

  override def addEdges(routes: Iterable[Route]): Seq[Edge[Integer, Integer]] = {
    val edges = env.fromCollection(routes)
      .map(j => new Edge(j.sourceAirportID, j.destAirportID, new Integer(0)))
    val addedEdges = edges.collect()
    graph = graph.addEdges(addedEdges.toList)
    addedEdges
    //println("Graph edges: " + graph.getEdges.count() + "\n")

    //    val dynamicPageRank = PageRankAlgorithm.runDynamic(graph, addedEdges, _fullPageRank, env).toSeq
    //    val classicPageRnnk = PageRankAlgorithm.runClassic(graph)
    //    println("Count dynamic: "+ dynamicPageRank.size + "; classic: " + classicPageRnnk.size)

    //    val cc = new ConnectedComponentsAlgorithm
    //    var result = cc.runDynamic(null, addedEdges)
    //
    //    result.foreach(println)

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