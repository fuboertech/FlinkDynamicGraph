package org.ilmenau.groupstudy.flinkdynamicgraph.algorithms

import org.apache.flink.api.scala._
import org.apache.flink.graph.Edge
import org.apache.flink.graph.scala.Graph
import org.apache.flink.types.DoubleValue
import org.apache.flink.api.scala._
import org.ilmenau.groupstudy.flinkdynamicgraph.graph.AbstractGraph
import org.apache.flink.api.java.{Utils, DataSet => JavaDataSet}
import org.ilmenau.groupstudy.flinkdynamicgraph.algorithms.util.PageRank


import scala.collection.JavaConverters._

class PageRankAlgorithm {


  private var _FullPageRank: Seq[(Integer, DoubleValue)] = _

  def runClassic(graph: Graph[Integer, Integer, Integer], border: org.apache.flink.api.java.DataSet[scala.Tuple2[Integer, DoubleValue]] = null): Seq[(Integer, DoubleValue)] = {
    val result = graph.run(new PageRank[Integer, Integer, Integer](0.85, 100, 0.001, border)).collect().asScala.toSeq
    _FullPageRank = result.map(f => Tuple2[Integer, DoubleValue](f.getVertexId0, f.getPageRankScore))
    _FullPageRank
  }

  // TODO: Not working properly 
  def runDynamic(graph: Graph[Integer, Integer, Integer], addedEdges: Seq[Edge[Integer, Integer]], env: ExecutionEnvironment): Seq[(Integer, DoubleValue)] = {
    val output = new StringBuilder

    var vc: Seq[Integer] = Seq.range(12, 20).map(i => new Integer(i))
    //addedEdges.map(e => e.getSource).distinct//union(addedEdges.map(e => e.getSource)).distinct
    var vb: Seq[Integer] = Seq()
    //graph.getEdges.filter(e => vc.contains(e.getTarget)).map(e => e.getSource).collect().diff(vc).distinct
    var vq: Seq[Integer] = Seq()
    //vc.distinct
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

    _FullPageRank.foreach(p => {
      if (vu.contains(p._1)) {
        p._2.setValue(p._2.getValue * _FullPageRank.size / graph.numberOfVertices())
        println("vuuu: " + p._1)
      }
    })

    _FullPageRank.foreach(p => {
      if (vb.contains(p._1))
        p._2.setValue(p._2.getValue * graph.numberOfVertices() / vq.size)
    })

    val q = vq.union(vb).distinct
    output.append("\nq: " + q.sortBy(f => f.intValue()))

    val subgraph: Graph[Integer, Integer, Integer] = graph.subgraph(v => q.contains(v.getId),
      e => q.contains(e.getSource) && q.contains(e.getTarget))
    output.append("\nsgv:" + subgraph.getVertices.collect().toString())
    output.append("\nsge:" + subgraph.getEdges.collect().toString())
    output.append("\ngrv: " + graph.getEdges.collect().toString())
    //println(output.result())

//    firstPageRank.foreach(p => {
//      if (vb.contains(p._1))
//        p._2.setValue(p._2.getValue * 0.4403279992)
//    })
    val scalaDataSetClass = classOf[DataSet[scala.Tuple2[Integer, DoubleValue]]]

    val toJavaDataSetMethod = scalaDataSetClass.getDeclaredMethod("javaSet") // no parameters
    toJavaDataSetMethod.setAccessible(true)
    val border:  DataSet[scala.Tuple2[Integer, DoubleValue]] = env.fromCollection(_FullPageRank.filter(p => vb.contains(p._1)))

    val subgraphPageRank = runClassic(subgraph, toJavaDataSetMethod.invoke(border).asInstanceOf[org.apache.flink.api.java.DataSet[scala.Tuple2[Integer, DoubleValue]]]).filterNot(p => vb.contains(p._1))

//    firstPageRank.foreach(p => {
//      if (vu.union(vb).contains(p._1))
//        p._2.setValue(p._2.getValue * 0.4375)
//    })

    _FullPageRank.foreach(p => {
      if (vb.contains(p._1))
        p._2.setValue(p._2.getValue * vq.size /graph.numberOfVertices())
    })

    subgraphPageRank.foreach(p => {
        p._2.setValue(p._2.getValue * vq.size /graph.numberOfVertices())
    })

    //val subgraphResult = subgraphPageRank.filterNot(x => vb.contains(x._1)).foreach(x => x._2.setValue(x._2.getValue * scale))

    val fullPageRank = _FullPageRank.filterNot(t => subgraphPageRank.map(p => p._1).contains(t._1)).union(subgraphPageRank)

//    _FullPageRank.foreach(println)
//    println()
//    subgraphPageRank.foreach(println)

    _FullPageRank = fullPageRank
    fullPageRank
  }

}
