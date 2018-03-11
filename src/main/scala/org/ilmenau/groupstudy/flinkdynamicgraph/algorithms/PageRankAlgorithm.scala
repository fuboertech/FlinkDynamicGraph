package org.ilmenau.groupstudy.flinkdynamicgraph.algorithms

import org.apache.flink.graph.{Edge, Vertex}
import org.ilmenau.groupstudy.flinkdynamicgraph.algorithms.PageRank
import org.apache.flink.graph.scala.Graph
import org.apache.flink.types.DoubleValue
import org.apache.flink.api.scala._
import org.ilmenau.groupstudy.flinkdynamicgraph.graph.AbstractGraph
import org.apache.flink.api.java.DataSet

import scala.collection.JavaConverters._

object PageRankAlgorithm {

  def runClassic(graph: Graph[Integer, Double, Integer], border: java.util.List[scala.Tuple2[Integer, DoubleValue]] = null): Seq[(Integer, DoubleValue)]  = {
    val result = graph.run(new org.ilmenau.groupstudy.flinkdynamicgraph.algorithms.PageRank[Integer, Double, Integer](0.85,  100, border)).collect().asScala.toSeq
    result.map(f => Tuple2[Integer, DoubleValue](f.getVertexId0,  f.getPageRankScore))
  }

  // TODO: Not working properly 
  def runDynamic(graph: Graph[Integer, Double, Integer], addedEdges: Seq[Edge[Integer, Integer]], firstPageRank: Seq[(Integer, DoubleValue)]): Seq[(Integer, DoubleValue)] = {
    val output = new StringBuilder

    var vc: Seq[Integer] = Seq.range(12,20).map(i=>new Integer(i))//addedEdges.map(e => e.getSource).distinct//union(addedEdges.map(e => e.getSource)).distinct
    var vb: Seq[Integer] = Seq()//graph.getEdges.filter(e => vc.contains(e.getTarget)).map(e => e.getSource).collect().diff(vc).distinct
    var vq: Seq[Integer] = Seq()//vc.distinct
    var vu = graph.getVertices.filter(v => !vc.contains(v.getId)).map(v => v.getId).collect()
    output.append("\nvc: " + vc.sortBy(f => f.intValue()))
    while (vc.nonEmpty) {
      vq = vq.union(vc).distinct
      vc = graph.getEdges
        .filter(e => vc.contains(e.getSource))
        .filter(e => vu.contains(e.getTarget))
        .map(e => e.getTarget).distinct().collect()
      output.append("\nvu: " + vu.sortBy(f => f.intValue()))
      output.append("\nvc: " + vc.sortBy(f => f.intValue()))
      vu = vu.diff(vc)
    }

    output.append("\nvu: " + vu.sortBy(f => f.intValue()))
    output.append("\nvq: " + vq.sortBy(f => f.intValue()))
    val childrens = graph.getEdges
      .filter(e => vu.contains(e.getSource))
      .filter(e => vq.contains(e.getTarget))
      .map(e => e.getSource).distinct().collect()

    vu = vu.diff(childrens)
    vb = vb.union(childrens).distinct
    output.append("\nvu: " + vu.sortBy(f => f.intValue()))
    output.append("\nvb: " + vb.sortBy(f => f.intValue()))

    val q = vq.union(vb).distinct
    output.append("\nq: " + q.sortBy(f => f.intValue()))
    val subgraph: Graph[Integer, Double, Integer] = graph.subgraph(v => q.contains(v.getId),
        e => q.contains(e.getSource) && q.contains(e.getTarget))
    output.append("\nsgv:"+subgraph.getVertices.collect().toString())
    output.append("\nsge:"+subgraph.getEdges.collect().toString())
    output.append("\ngrv: "+graph.getEdges.collect().toString())
    println(output.result())

    firstPageRank.foreach(p => {
      if (vu.union(vb).contains(p._1))
        p._2.setValue(p._2.getValue * 0.4403279992)
    })
    val subgraphPageRank = runClassic(subgraph, firstPageRank.filter(p => vb.contains(p._1)).asJava).filterNot(p => vb.contains(p._1))

    //val subgraphResult = subgraphPageRank.filterNot(x => vb.contains(x._1)).foreach(x => x._2.setValue(x._2.getValue * scale))

    val fullPageRank = firstPageRank.filterNot(t => subgraphPageRank.map(p => p._1).contains(t._1)).union(subgraphPageRank)

    firstPageRank.foreach(println)
    println()
    subgraphPageRank.foreach(println)

    fullPageRank
  }

}
