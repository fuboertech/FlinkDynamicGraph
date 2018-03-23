package org.ilmenau.groupstudy.flinkdynamicgraph.algorithms

import org.apache.flink.api.scala._
import org.apache.flink.graph.Edge
import org.apache.flink.graph.scala.Graph
import org.apache.flink.types.DoubleValue
import org.ilmenau.groupstudy.flinkdynamicgraph.model.data.Airport
import org.apache.flink.api.java.{Utils, DataSet => JavaDataSet}
import org.ilmenau.groupstudy.flinkdynamicgraph.algorithms.util.PageRank

import scala.collection.JavaConverters._

class PageRankAlgorithm {

  private var _FullPageRank: Seq[(Integer, DoubleValue)] = _

  def runClassic(graph: Graph[Integer, Integer, Integer], border: org.apache.flink.api.java.DataSet[scala.Tuple2[Integer, DoubleValue]] = null, savePageRank: Boolean = true): Seq[(Integer, DoubleValue)] = {
    val result = graph.run(new PageRank[Integer, Integer, Integer](0.85, 100, 0.001, border)).collect().asScala.toSeq
    val pageRank = result.map(f => Tuple2[Integer, DoubleValue](f.getVertexId0, f.getPageRankScore))
    if (savePageRank) {
      _FullPageRank = pageRank
    }
    pageRank
  }

  def runDynamic(graph: Graph[Integer, Integer, Integer], addedEdges: Seq[Edge[Integer, Integer]], env: ExecutionEnvironment): Seq[(Integer, DoubleValue)] = {
    val output = new StringBuilder
    var vc: Seq[Integer] = addedEdges.map(x => x.getSource).distinct //Seq.range(12, 20).map(i => new Integer(i))//
    var vb: Seq[Integer] = Seq()
    var vq: Seq[Integer] = Seq()
    var vu = graph.getVertexIds().filter(v => !vc.contains(v)).collect()
//    output.append("\nvc: " + vc.sortBy(f => f.intValue()))
    while (vc.nonEmpty) {
      println("size:" + vc.size)
      vq = vq.union(vc).distinct
      vc = graph.getEdgeIds()
        .filter(e => vc.contains(e._1)).filter(e => vu.contains(e._2))
        .map(e => e._2).distinct().collect()
//      output.append("\nvu: " + vu.sortBy(f => f.intValue()))
//      output.append("\nvc: " + vc.sortBy(f => f.intValue()))
      vu = vu.diff(vc)
    }
//    output.append("\nvu: " + vu.sortBy(f => f.intValue()))
//    output.append("\nvq: " + vq.sortBy(f => f.intValue()))
    val childrens = graph.getEdgeIds()
      .filter(e => vq.contains(e._2) && vu.contains(e._1))
      .map(e => e._1).distinct().collect()
    vu = vu.diff(childrens)
    vb = vb.union(childrens).distinct
//    output.append("\nvu: " + vu.sortBy(f => f.intValue()))
//    output.append("\nvb: " + vb.sortBy(f => f.intValue()))

//    _FullPageRank.foreach(p => {
//      if (vu.contains(p._1)) {
//        p._2.setValue(p._2.getValue * _FullPageRank.size / graph.numberOfVertices())
//        println("vuuu: " + p._1)
//      }
//    })
//
//    _FullPageRank.foreach(p => {
//      if (vb.contains(p._1))
//        p._2.setValue(p._2.getValue * graph.numberOfVertices() / vq.size)
//    })

    val q = vq.union(vb).distinct
//    output.append("\nq: " + q.sortBy(f => f.intValue()))
    val subgraph: Graph[Integer, Integer, Integer] = graph.subgraph(v => q.contains(v.getId),
      e => q.contains(e.getSource) && q.contains(e.getTarget))
//    output.append("\nsgv:" + subgraph.getVertices.collect().toString())
//    output.append("\nsge:" + subgraph.getEdges.collect().toString())
//    output.append("\ngrv: " + graph.getEdges.collect().toString())
    //println(output.result())

    val scalaDataSetClass = classOf[DataSet[scala.Tuple2[Integer, DoubleValue]]]

    val toJavaDataSetMethod = scalaDataSetClass.getDeclaredMethod("javaSet") // no parameters
    toJavaDataSetMethod.setAccessible(true)
    val border:  DataSet[scala.Tuple2[Integer, DoubleValue]] = env.fromCollection(_FullPageRank.filter(p => vb.contains(p._1)))

    val subgraphPageRank = runClassic(subgraph, toJavaDataSetMethod.invoke(border).asInstanceOf[org.apache.flink.api.java.DataSet[scala.Tuple2[Integer, DoubleValue]]], false).filterNot(p => vb.contains(p._1))

//    _FullPageRank.foreach(p => {
//      if (vb.contains(p._1))
//        p._2.setValue(p._2.getValue * vq.size /graph.numberOfVertices())
//    })

//    subgraphPageRank.foreach(p => {
//        p._2.setValue(p._2.getValue * vq.size /graph.numberOfVertices())
//    })

    val fullPageRank = _FullPageRank.filterNot(t => subgraphPageRank.map(p => p._1).contains(t._1)).union(subgraphPageRank)
    _FullPageRank = fullPageRank
    fullPageRank
  }

}
