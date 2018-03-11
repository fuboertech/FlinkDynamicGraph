package org.ilmenau.groupstudy.flinkdynamicgraph.algorithms

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.graph.scala._
import org.apache.flink.graph.spargel.{GatherFunction, MessageIterator, ScatterFunction}
import org.apache.flink.graph.{Edge, Vertex}
import org.apache.flink.api.java.DataSet
import org.apache.flink.types.DoubleValue

import scala.collection.JavaConverters._


object ShortestPathAlgorithm {
  def run(graph: Graph[Integer, Double, Integer]): Seq[(Integer, DoubleValue)] = {
    // Execute the scatter-gather iteration
    val result = graph.runScatterGatherIteration(new MinDistanceMessenger, new VertexDistanceUpdater, 3)

    // Extract the vertices as the result
    val singleSourceShortestPaths = result.getVertices

    singleSourceShortestPaths.print()

    singleSourceShortestPaths.collect().toSeq.map(f => Tuple2[Integer, DoubleValue](f.getId, new DoubleValue(f.getValue)))
  }
  

  // --------------------------------------------------------------------------------------------
  //  Single Source Shortest Path UDFs
  // --------------------------------------------------------------------------------------------



  /**
    * Distributes the minimum distance associated with a given vertex among all
    * the target vertices summed up with the edge's value.
    */
  private final class MinDistanceMessenger extends ScatterFunction[Integer, Double, Double, Integer] {

    override def sendMessages(vertex: Vertex[Integer, Double]) {
      if (vertex.getValue < Double.PositiveInfinity) {
        for (edge: Edge[Integer, Integer] <- getEdges.asScala) {
//          println(edge.getTarget)
          if(edge.getTarget != -1)
            sendMessageTo(edge.getTarget, vertex.getValue + edge.getValue)
        }
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