package org.ilmenau.groupstudy.flinkdynamicgraph.graph

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.graph.scala.Graph
import org.apache.flink.graph.utils.GraphUtils.IdentityMapper
import org.apache.flink.graph.{Edge, Vertex}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.types.DoubleValue
import org.ilmenau.groupstudy.flinkdynamicgraph.algorithms.PageRankAlgorithm
import org.ilmenau.groupstudy.flinkdynamicgraph.loader.DataLoader
import org.ilmenau.groupstudy.flinkdynamicgraph.model.data.{Airport, Route}
//import org.apache.flink.api.scala._
import org.ilmenau.groupstudy.flinkdynamicgraph.model.{ChangeModel, ChangesModel}

class TestGraph(env: ExecutionEnvironment) extends AbstractGraph(env: ExecutionEnvironment) {

  private var _fullPageRank: Seq[(Integer, Integer)] = _

  var edges: DataSet[Edge[Integer, Integer]] = _

  def construct(): Unit = {
    val start = 1
    val end   = 5
    val rnd = new scala.util.Random
    edges = DataLoader.testGraphEdtes.map(j => new Edge(j.sourceAirportID, j.destAirportID, new Integer(start + rnd.nextInt( (end - start) + 1 ))))

    // from: source airport id, to: dest airport id, value: airlineID

    val vertices = env.fromCollection(Seq.range(1,12).union(Seq.range(20,24)))
      .map(a => new Vertex(new Integer(a), new Integer(0)))


    val e = edges.filter(e => !Seq.range(12,20).map(i=>new Integer(i)).contains(e.getSource) &&
      !Seq.range(12,20).map(i=>new Integer(i)).contains(e.getTarget))


    graph = Graph.fromDataSet(vertices, e, env)
    //_fullPageRank = PageRankAlgorithm.runClassic(graph)
  }

  override def addEdges(routes: Iterable[Route]): Seq[Edge[Integer, Integer]] = {
    val e = edges.filter(e => Seq.range(12, 20).map(i=>new Integer(i)).contains(e.getSource) ||
      Seq.range(12, 20).map(i=>new Integer(i)).contains(e.getTarget)).collect()
    val v = env.fromCollection(Seq.range(12,20).map(i=>new Integer(i))
      .map(a => new Vertex(a, new Integer(0)))).collect()

    graph = graph.addVertices(v.toList).addEdges(e.toList).subgraph(v => true, e => true)
    println("Graph edges: " + graph.getEdges.count() + "\n")

    //val cm = ChangesModel[Seq[Edge[Integer, Integer]],Seq[Vertex[Integer, Airport]]](ChangeModel(e,v), null)
    val pr = new PageRankAlgorithm()
    val classicPageRnnk = pr.runClassic(graph)
    val dynamicPageRank = pr.runDynamic(graph, e, env)
    println("dyn: " + dynamicPageRank.size + "; classic: " + classicPageRnnk.size)

    env.fromCollection(dynamicPageRank).join(env.fromCollection(classicPageRnnk)).where(0).equalTo(0) {
      (dynamic, classic) =>
        if (!dynamic._2.equals(classic._2)) {
          println(dynamic +"] != [" + classic)
        }
        (dynamic._1, dynamic._2, classic._2)
    }.collect()

    e.toSeq
  }

}