package org.ilmenau.groupstudy.flinkdynamicgraph.algorithms

import org.apache.flink.graph.Edge
import org.apache.flink.graph.library.link_analysis.PageRank
import org.apache.flink.graph.scala.Graph
import org.apache.flink.types.DoubleValue
import org.apache.flink.api.scala._
import org.ilmenau.groupstudy.flinkdynamicgraph.graph.AbstractGraph
import org.ilmenau.groupstudy.flinkdynamicgraph.model.data.Airport

import scala.collection.JavaConverters._

object PageRankAlgorithm {

  def runClassic(graph: Graph[Integer, Airport, Integer]): Seq[(Integer, DoubleValue)] = {
    val result = graph.run(new PageRank[Integer, Airport, Integer](0.85,  20, 0.001)).collect().asScala.toSeq
    result.map(f => Tuple2[Integer, DoubleValue](f.getVertexId0,  f.getPageRankScore))
  }

  // TODO: Not working properly 
  def runDynamic(graph: AbstractGraph, addedEdges: Seq[Edge[Integer, Integer]], firstPageRank: Seq[(Integer, DoubleValue)]): Seq[(Integer, DoubleValue)] = {
    var vc: Seq[Integer] = addedEdges.map(e => e.getTarget).union(addedEdges.map(e => e.getSource)).distinct
    var vb: Seq[Integer] = graph.get.getEdges.filter(e => vc.contains(e.getTarget)).map(e => e.getSource).collect().distinct
    var vq: Seq[Integer] = vc.distinct
    var vu = graph.get.getVertices.filter(v => !vc.contains(v.getId)).map(v => v.getId).collect()
    while (vc.nonEmpty) {
      vc = graph.get.getEdges
        .filter(e => vc.contains(e.getSource))
        .filter(e => vu.contains(e.getTarget))
        .map(e => e.getTarget).distinct().collect()
      vu = vu.filterNot(v => vc.contains(v))
      vq = vq.union(vc).distinct
    }
    val refs = vu.filter(v => vq.contains(v))
    vu = vu.filterNot(v => refs.contains(v))
    vb = vb.union(refs)

    val q = vq.union(vb)
    val subgraph: Graph[Integer, Airport, Integer] = graph.get.subgraph(v => q.contains(v.getId),
      e => q.contains(e.getSource) && q.contains(e.getTarget))

    val subgraphPageRank = runClassic(subgraph)
    val fullPageRank = firstPageRank.filterNot(t => subgraphPageRank.map(p => p._1).contains(t._1)).union(subgraphPageRank)
    fullPageRank
  }

}
