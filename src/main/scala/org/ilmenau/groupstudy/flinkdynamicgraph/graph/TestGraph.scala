package org.ilmenau.groupstudy.flinkdynamicgraph.graph

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.graph.scala.Graph
import org.apache.flink.graph.{Edge, Vertex}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.types.DoubleValue
import org.ilmenau.groupstudy.flinkdynamicgraph.algorithms.PageRankAlgorithm
import org.ilmenau.groupstudy.flinkdynamicgraph.loader.DataLoader
import org.ilmenau.groupstudy.flinkdynamicgraph.model.data.{Airport, Route}
//import org.apache.flink.api.scala._
import org.ilmenau.groupstudy.flinkdynamicgraph.model.{ChangeModel, ChangesModel}

class TestGraph(env: ExecutionEnvironment) extends AbstractGraph(env: ExecutionEnvironment) {

  private var _fullPageRank: Seq[(Integer, DoubleValue)] = _

  var edges: DataSet[Edge[Integer, Integer]] = _

  def construct(): Unit = {

//    edges = DataLoader.testGraphEdtes.map(j => new Edge(j.sourceAirportID, j.destAirportID, j.airlineID))

    // from: source airport id, to: dest airport id, value: airlineID
    val vertices = env.fromCollection(Seq.range(1,12).union(Seq.range(20,24)))
      .map(a => new Vertex(new Integer(a), Double.PositiveInfinity))


    val e = edges.filter(e => !Seq.range(12,20).map(i=>new Integer(i)).contains(e.getSource) &&
      !Seq.range(12,20).map(i=>new Integer(i)).contains(e.getTarget))

    graph = Graph.fromDataSet[Integer, Double, Integer](vertices, edges, env)
    _fullPageRank = PageRankAlgorithm.runClassic(graph)
  }

  override def addEdges(routes: Iterable[Route]): Unit = {
    val e = edges.filter(e => Seq.range(12, 20).map(i=>new Integer(i)).contains(e.getSource) ||
      Seq.range(12, 20).map(i=>new Integer(i)).contains(e.getTarget)).collect()
    val v = env.fromCollection(Seq.range(12,20).map(i=>new Integer(i))
      .map(a => new Vertex(a, 0.0))).collect()

    graph = graph.addVertices(v.toList).addEdges(e.toList).subgraph(v => true, e => true)
    println("Graph edges: " + graph.getEdges.count() + "\n")

    //val cm = ChangesModel[Seq[Edge[Integer, Integer]],Seq[Vertex[Integer, Airport]]](ChangeModel(e,v), null)
    val dynamicPageRank = PageRankAlgorithm.runDynamic(graph, e, _fullPageRank)
    val classicPageRnnk = PageRankAlgorithm.runClassic(graph)
    println("dyn: " + dynamicPageRank.size + "; classic: " + classicPageRnnk.size)

    env.fromCollection(dynamicPageRank).join(env.fromCollection(classicPageRnnk)).where(0).equalTo(0) {
      (dynamic, classic) =>
        if (!dynamic._2.equals(classic._2)) {
          println(dynamic +"] != [" + classic)
        }
        (dynamic._1, dynamic._2, classic._2)
    }.collect()
  }

}
