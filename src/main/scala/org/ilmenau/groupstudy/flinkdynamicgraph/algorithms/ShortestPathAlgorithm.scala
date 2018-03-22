package org.ilmenau.groupstudy.flinkdynamicgraph.algorithms

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.scala.DataSet
import org.apache.flink.graph.scala._
import org.apache.flink.api.scala._
import org.apache.flink.graph.spargel.{GatherFunction, MessageIterator, ScatterFunction}
import org.apache.flink.graph.utils.GraphUtils
import org.apache.flink.graph.{Edge, Vertex, VertexJoinFunction}
import org.apache.flink.graph.VertexJoinFunction
//import org.apache.flink.graph.pregel.{ComputeFunction, MessageCombiner, MessageIterator}
import  org.apache.flink.streaming.api.scala.createTypeInformation

import org.apache.flink.types.DoubleValue



import scala.collection.JavaConverters._



object ShortestPathAlgorithm {
  def runClassic(graph: Graph[Integer, Double, Integer]): Seq[(Integer, DoubleValue)] = {

    val result = run(graph)
    val singleSourceShortestPaths = result.getVertices
    singleSourceShortestPaths.print()

    singleSourceShortestPaths.collect().toSeq.map(f => Tuple2[Integer, DoubleValue](f.getId, new DoubleValue(f.getValue)))
  }

  def run(graph: Graph[Integer, Double, Integer]): Graph[Integer, Double, Integer] = {
    graph.getEdges.print()
    val maxIterations = 5

    val result = graph.runScatterGatherIteration(new MinDistanceMessenger, new VertexDistanceUpdater, maxIterations)

    result.getVertices

    //     val result = graph.runVertexCentricIteration(new SSSPComputeFunction, new SSSPCombiner, maxIterations)
    //    val result = graph.runVertexCentricIteration(new SSSPComputeFunction, null, maxIterations)

    return result
  }

  def runDynamic(graph: Graph[Integer, Double, Integer], addedEdges: Seq[Edge[Integer, Integer]]) = {
    var modifiedGraph = graph

    addedEdges.foreach(f => modifiedGraph = processEdgeWeightUpdate(modifiedGraph, f))

    modifiedGraph.getVertices.collect().toSeq.map(f => Tuple2[Integer, DoubleValue](f.getId, new DoubleValue(f.getValue)))
  }

  def processEdgeWeightUpdate(graph: Graph[Integer, Double, Integer], edge: Edge[Integer, Integer]): Graph[Integer, Double, Integer] ={
    var modifiedGraph = graph

    val newShortestPath = graph.getTriplets().filter(f => f.getTrgVertex.getId == edge.getTarget)
        .map(x => x.getEdge.getValue + x.getSrcVertex.getValue).collect().min


    val currentShortestPath = modifiedGraph.getVertices
      .filter(x => x.getId == edge.getTarget || x.getId == edge.getSource)
      .collect().head

    if(newShortestPath != currentShortestPath.getValue){
//      val srcVerticesIdsToRecalcPaths = modifiedGraph.getVertices
//        .filter(x => x.getId == edge.getSource)
//        .filter(x => x.getValue != Double.PositiveInfinity && x.getId == edge.getTarget)
//        .map(x => x.getId)
//        .collect()


//      val dstVerticesIdsToRecalcPaths = modifiedGraph.getVertices
//        .filter(x => x.getValue != Double.PositiveInfinity && x.getId == edge.getTarget)
//        .filter(x => x.getId != edge.getSource)
//        .map(x => x.getId)
//        .collect()

//
//      modifiedGraph = modifiedGraph.joinWithVertices(modifiedGraph.getVertices.map((e: Vertex[Integer, Double]) => Tuple2(e.getId, e.getValue)), (currVal: Double, newVal: Tuple2[Integer, Double]) =>
//        if(dstVerticesIdsToRecalcPaths.contains(newVal._1)) {
//          if(srcVerticesIdsToRecalcPaths.contains(currVal))
//            Double.PositiveInfinity
//          else
//            newVal._2
//        } else
//          currVal
//      )

      return run(modifiedGraph)
    } else {
      return  modifiedGraph
    }
  }

//  def runDynamic(graph: Graph[Integer, Double, Integer], addedEdges: Seq[Edge[Integer, Integer]]): Seq[(Integer, DoubleValue)] = {
//    val output = new StringBuilder
//
//    //var vc: Seq[Integer] = Seq.range(12, 20).map(i => new Integer(i))
//    var vc: Seq[Integer] = addedEdges.map(e => e.getSource).distinct//union(addedEdges.map(e => e.getSource)).distinct
//    var vb: Seq[Integer] = Seq()
//    //graph.getEdges.filter(e => vc.contains(e.getTarget)).map(e => e.getSource).collect().diff(vc).distinct
//    var vq: Seq[Integer] = Seq()
//    //vc.distinct
//    var vu = graph.getVertices.filter(v => !vc.contains(v.getId)).map(v => v.getId).collect()
//    output.append("\nvc: " + vc.sortBy(f => f.intValue()))
//    while (vc.nonEmpty) {
//      vq = vq.union(vc).distinct
//      vc = graph.getEdges
//        .filter(e => vc.contains(e.getSource))
//        .filter(e => vu.contains(e.getTarget))
//        .map(e => e.getTarget).distinct().collect()
//      output.append("\nvu: " + vu.sortBy(f => f.intValue()))
//      output.append("\nvc: " + vc.sortBy(f => f.intValue()))
//      vu = vu.diff(vc)
//    }
//
//
//
//    output.append("\nvu: " + vu.sortBy(f => f.intValue()))
//    output.append("\nvq: " + vq.sortBy(f => f.intValue()))
//    val childrens = graph.getEdges
//      .filter(e => vu.contains(e.getSource))
//      .filter(e => vq.contains(e.getTarget))
//      .map(e => e.getSource).distinct().collect()
//
//    vu = vu.diff(childrens)
//    vb = vb.union(childrens).distinct
//    output.append("\nvu: " + vu.sortBy(f => f.intValue()))
//    output.append("\nvb: " + vb.sortBy(f => f.intValue()))
//
//    val q = vq.union(vb).distinct
//    output.append("\nq: " + q.sortBy(f => f.intValue()))
//
//    val subgraph: Graph[Integer, Double, Integer] = graph.subgraph(v => q.contains(v.getId),
//      e => q.contains(e.getSource) && q.contains(e.getTarget))
//
//    output.append("\nsgv:" + subgraph.getVertices.collect().toString())
//    output.append("\nsge:" + subgraph.getEdges.collect().toString())
//    output.append("\ngrv: " + graph.getEdges.collect().toString())
//
//    println(output.result())
//
//    //    firstPageRank.foreach(p => {
//    //      if (vb.contains(p._1))
//    //        p._2.setValue(p._2.getValue * 0.4403279992)
//    //    })
//
//    val maxIterations = 5
//
//    val result = subgraph.runScatterGatherIteration(new MinDistanceMessenger, new VertexDistanceUpdater, maxIterations)
//    val singleSourceShortestPaths = result.getVertices
//    result.getVertices
//    singleSourceShortestPaths.print()
//    //     val result = graph.runVertexCentricIteration(new SSSPComputeFunction, new SSSPCombiner, maxIterations)
//    //    val result = graph.runVertexCentricIteration(new SSSPComputeFunction, null, maxIterations)
//
//
//    singleSourceShortestPaths.collect().toSeq.map(f => Tuple2[Integer, DoubleValue](f.getId, new DoubleValue(f.getValue)))
//  }

  // - - -  UDFs - - - //

//  final class SSSPComputeFunction extends ComputeFunction[Integer, Double, Integer, Double] {
//
//    override def compute(vertex: Vertex[Integer, Double], messages: MessageIterator[Double]) = {
//      var minDistance = if (vertex.getId.equals(srcId)) 0 else Double.MaxValue
//
//      while (messages.hasNext) {
//        val msg = messages.next
//        if (msg < minDistance) {
//          minDistance = msg
//        }
//      }
//
//      if (vertex.getValue > minDistance) {
//        setNewVertexValue(minDistance)
//        for (edge: Edge[Integer, Integer] <- getEdges.asScala) {
//          sendMessageTo(edge.getTarget, vertex.getValue + edge.getValue)
//        }
//      }
//    }
//  }
//
//      // message combiner
//      final class SSSPCombiner extends MessageCombiner[Integer, Double] {
//
//        override def combineMessages(messages: MessageIterator[Double]) {
//
//          var minDistance = Double.MaxValue
//
//          while (messages.hasNext) {
//            val msg = messages.next
//            if (msg < minDistance) {
//              minDistance = msg
//            }
//          }
//          sendCombinedMessage(minDistance)
//        }
//      }


//  /**
//    * Distributes the minimum distance associated with a given vertex among all
//    * the target vertices summed up with the edge's value.
//    */
  private final class MinDistanceMessenger extends ScatterFunction[Integer, Double, Double, Integer] {

    override def sendMessages(vertex: Vertex[Integer, Double]) {
      if (vertex.getValue == Double.PositiveInfinity)
        vertex.setValue(0.0)

      for (edge: Edge[Integer, Integer] <- getEdges.asScala) {
        sendMessageTo(edge.getTarget, vertex.getValue + edge.getValue)
      }
    }
  }

  /**
    * Function that updates the value of a vertex by picking the minimum
    * distance from all incoming messages.
    */
  private final class VertexDistanceUpdater extends GatherFunction[Integer, Double, Double] {

    override def updateVertex(vertex: Vertex[Integer, Double], inMessages: MessageIterator[Double]) {
      var minDistance = Double.MaxValue
      while (inMessages.hasNext) {
        val msg = inMessages.next
        if (msg < minDistance) {
          minDistance = msg
        }
      }
      if (vertex.getValue > minDistance) {
        setNewVertexValue(minDistance)
      }
    }
  }
}


//class ShortestPathAlgorithm {
//
//  def DynamicApsp(graph: Graph[Array[(VertexId, Double)], Double], changes: GraphChangesModel[Array[(VertexId, Double)], Double]): Graph[Array[(VertexId, Double)], Double] = {
//    var modifiedGraph = ProcessVerticesAdd(sc, graph,
//      changes.vertices.added.map(x => x._1))
//
//    val edgesChanges = changes.edges.added++
//      changes.edges.deleted
//        .map(x => Edge[Double](x._1, x._2, Double.PositiveInfinity))++
//      changes.edges.changed++
//      modifiedGraph.edges
//        .filter(x => changes.vertices.deleted.contains(x.srcId) || changes.vertices.deleted.contains(x.dstId))
//        .collect()
//    for (modifiedEdge <- edgesChanges) {
//      modifiedGraph = ProcessEdgeWeightUpdate(sc, modifiedGraph, modifiedEdge)
//    }
//    modifiedGraph
//  }
//
//  def ProcessEdgeWeightUpdate(sc: SparkContext, graph: Graph[Array[(VertexId, Double)], Double], modifiedEdge: Edge[Double]): Graph[Array[(VertexId, Double)], Double] = {
//
//    var modifiedGraph = GraphUtils.UpdateEdges(sc, graph, Array(modifiedEdge))
//
//    // calc new shortest path from modified edge src to modified dst
//    val newShortestPath = modifiedGraph.triplets
//        .filter(x => x.dstId == modifiedEdge.dstId)
//        .map(x => x.attr + x.srcAttr.filter(y => y._1 == modifiedEdge.srcId).head._2)
//          .min()
//
//
//    val currentShortestPath = modifiedGraph.vertices
//      .filter(x => x._1 == modifiedEdge.dstId).collect().head
//      ._2
//      .filter(x => x._1 == modifiedEdge.srcId).head._2
//
//
//    if(newShortestPath != currentShortestPath){
//      val srcVerticesIdsToRecalcPaths = modifiedGraph.vertices
//        .filter(x => x._1 == modifiedEdge.srcId)
//        .flatMap(x => x._2)
//        .filter(x => x._2 != Double.PositiveInfinity &&
//          x._1 == modifiedEdge.dstId)
//        .map(x => x._1)
//        .collect()
//
//      val dstVerticesIdsToRecalcPaths = modifiedGraph.vertices
//          .flatMap(x => x._2.map(y => (y._1, x._1, y._2)))
//          .filter(x => x._3 != Double.PositiveInfinity &&
//            x._1 == modifiedEdge.dstId && x._2 != modifiedEdge.srcId)
//          .map(x => x._2)
//          .collect()
//
//      modifiedGraph = modifiedGraph
//          .joinVertices(modifiedGraph.vertices) ( (id, currVal, newVal) => {
//            if(dstVerticesIdsToRecalcPaths.contains(id)) {
//              currVal.map(x => if(srcVerticesIdsToRecalcPaths.contains(x._1))
//                (x._1, Double.PositiveInfinity) else x)
//                .sortBy(x => x._1)
//            } else {
//              currVal
//            }
//          })
//      return ClassicApsp(modifiedGraph)
//    } else {
//      return  modifiedGraph
//    }
//  }
//
//  def ClassicApsp(graph: Graph[Array[( VertexId, Double )], Double ]): Graph[Array[(VertexId, Double)], Double]= {
//
//    val initialMsg: Array[(VertexId , Double)] = graph.vertices
//      .map(x => (x._1, Double.PositiveInfinity )).collect().sortBy(x => x._1)
//
//    def vertexProgram(id: VertexId , attr: Array[(VertexId , Double)], msgSum: Array[( VertexId , Double )]): Array[(VertexId , Double)] = {
//      var result: Array[(VertexId, Double)] = null
//      if (msgSum.count(x => x._2 != Double .PositiveInfinity) > 0){
//        result = msgSum.map(x => (x._1, if(x._1 == id) 0 else x._2))
//          .sortBy(x => x._1)
//      } else {
//        val newMap = attr.zip(msgSum)
//          .map(x => (x._1._1, Math.min(x._2._2, x._1._2)))
//
//        result = newMap
//      }
//      result
//    }
//
//  def sendMessage(edge: EdgeTriplet[Array[(VertexId, Double)], Double]) = {
//    var msgMap = edge
//      .srcAttr.zip(edge.dstAttr)
//      .map(x => if(x._1._2 + edge.attr < x._2._2)
//          (x._1._1, x._1._2 + edge.attr)
//        else(x._1._1, Double.PositiveInfinity))
//      .sortBy(x => x._1)
//
//    if(msgMap.count(x => x._2 != Double.PositiveInfinity) > 0) {
//      Iterator((edge.dstId, (edge.dstId, msgMap)))
//    } else {
//      Iterator.empty
//    }
//  }
//
//  def messageCombiner(a: Array[(VertexId, Double)],
//                      b: Array[(VertexId, Double)]): Array[(VertexId, Double)] = {
//    val mixedArray = (a.zip(b)).map(x => (x._1._1, Math.min(x._1, x._2._2))).sortBy(x => x._1)
//
//    return mixedArray
//  }
//
//  return Pregel(graph, initialMsg) (VertexProgram, sendMessage, messageCombiner)
//}