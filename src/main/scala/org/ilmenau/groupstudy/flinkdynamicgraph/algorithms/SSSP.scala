package org.ilmenau.groupstudy.flinkdynamicgraph.algorithms

import org.apache.flink.api.common.functions.{FilterFunction, MapFunction}
import org.apache.flink.api.scala._
import org.apache.flink.graph.scala._
import org.apache.flink.graph.scala.utils.Tuple3ToEdgeMap
import org.apache.flink.graph.{Edge, Vertex}
import org.apache.flink.types.DoubleValue
import org.apache.flink.api.scala.DataSet
import org.apache.flink.graph.spargel.{GatherFunction, MessageIterator, ScatterFunction}

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer


object SSSP {
  var _dynamicSSSP: Graph[Integer, Integer, Integer] = _

  var changedVertices: ListBuffer[Vertex[Integer, Integer]] = new ListBuffer[Vertex[Integer, Integer]]

  def runClassic(graph: Graph[Integer, Integer, Integer]): Graph[Integer, Integer, Integer] = {
    val srcId = 1
    changedVertices = new ListBuffer[Vertex[Integer, Integer]]

    var modifiedGraph = graph

    modifiedGraph = modifiedGraph.mapVertices(v => if (v.getId.equals(srcId)) {
      0
    } else {
      Integer.MAX_VALUE
    })

    val maxIterations = 3
    val result = modifiedGraph.runScatterGatherIteration(new MinDistanceMessenger, new VertexDistanceUpdater, maxIterations)

    val singleSourceShortestPaths = result.getVertices
    singleSourceShortestPaths.print()

    return result
    //singleSourceShortestPaths.collect().toSeq.map(f => Tuple2[Integer, Integer](f.getId, f.getValue))
  }

  // --------------------------------------------------------------------------------------------
  //  Single Source Shortest Path UDFs
  // --------------------------------------------------------------------------------------------

  private final class MinDistanceMessenger extends ScatterFunction[Integer, Integer, Integer, Integer] {

    override def sendMessages(vertex: Vertex[Integer, Integer]) {
        for (edge: Edge[Integer, Integer] <- getEdges) {
          if (vertex.getValue < Integer.MAX_VALUE)
            sendMessageTo(edge.getTarget, vertex.getValue + edge.getValue)
        }
    }
  }

  /**
    * Function that updates the value of a vertex by picking the minimum
    * distance from all incoming messages.
    */
  private final class VertexDistanceUpdater extends GatherFunction[Integer, Integer, Integer] {

    override def updateVertex(vertex: Vertex[Integer, Integer], inMessages: MessageIterator[Integer]) {
      var minDistance = vertex.getValue
      while (inMessages.hasNext) {
        val msg = inMessages.next
        if (msg < minDistance) {
          minDistance = msg
        }
      }
      if (vertex.getValue > minDistance) {
        setNewVertexValue(minDistance)
        changedVertices += vertex
      }
    }
  }

  // dynamic
  def runDynamic(graph: Graph[Integer, Integer, Integer], addedEdges: Seq[Edge[Integer, Integer]]): Graph[Integer, Integer, Integer] = {
    val srcId = 1
    val maxIterations = 3

    changedVertices = new ListBuffer[Vertex[Integer, Integer]]

    var modifiedGraph = graph
    if (_dynamicSSSP == null) {
      modifiedGraph = graph.mapVertices(v => if (v.getId.equals(srcId)) {
        0
      } else {
        Integer.MAX_VALUE
      })

      val a = modifiedGraph.getVertices.count()
      val b = modifiedGraph.getVertices.filter(v => v.getValue == Integer.MAX_VALUE).count()

      if(b == a-1){
        modifiedGraph = modifiedGraph.runScatterGatherIteration(new MinDistanceMessenger, new VertexDistanceUpdater, maxIterations)
      }

    } else {
      var savedVertices = _dynamicSSSP.getVertices.map(vv => (vv.getId, vv.getValue))
      modifiedGraph = graph.joinWithVertices(savedVertices, (graphVal: Integer, savedVal: Integer) => savedVal)
    }

    modifiedGraph = modifiedGraph.runScatterGatherIteration(new RecalculateMessenger(addedEdges), new VertexDistanceUpdater, maxIterations)
    _dynamicSSSP = modifiedGraph

    modifiedGraph.getVertices.print()


    //modifiedGraph.getVertices.collect().toSeq.map(f => Tuple2[Integer, Integer](f.getId, f.getValue))
    return  modifiedGraph
  }

  private final class RecalculateMessenger(var edgesToBeChanged: Seq[Edge[Integer, Integer]]) extends ScatterFunction[Integer, Integer, Integer, Integer] {
    override def sendMessages(vertex: Vertex[Integer, Integer]) {
      if (vertex.getValue < Integer.MAX_VALUE) {
        for (edge <- edgesToBeChanged) {
          if (vertex.getId.equals(edge.getSource)) {
            sendMessageTo(edge.getTarget, vertex.getValue + edge.getValue)
          }
        }

        if(changedVertices.contains(vertex)){
          changedVertices -= vertex
          for(edge <- getEdges)
            sendMessageTo(edge.getTarget, vertex.getValue + edge.getValue)
        }
      }
    }
  }
}


