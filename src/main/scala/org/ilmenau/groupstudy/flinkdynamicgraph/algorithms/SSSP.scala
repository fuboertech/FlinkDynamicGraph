package org.ilmenau.groupstudy.flinkdynamicgraph.algorithms

import org.apache.flink.api.common.functions.{FilterFunction, MapFunction}
import org.apache.flink.api.scala._
import org.apache.flink.graph.scala._
import org.apache.flink.graph.scala.utils.Tuple3ToEdgeMap
import org.apache.flink.graph.{Edge, Vertex}
import org.apache.flink.types.DoubleValue
import org.apache.flink.graph.spargel.{GatherFunction, MessageIterator, ScatterFunction}

import scala.collection.JavaConversions._


object SSSP {
  def runClassic(graph: Graph[Integer, Integer, Integer]): Seq[(Integer, Integer)] = {
    val srcId = 1

    var modifiedGraph = graph
    modifiedGraph = graph.mapVertices(v => if (v.getId.equals(srcId)) {
      0
    } else {
      Integer.MAX_VALUE
    })

    val maxIterations = 5
    val result = modifiedGraph.runScatterGatherIteration(new MinDistanceMessenger, new VertexDistanceUpdater, maxIterations)

    val singleSourceShortestPaths = result.getVertices
    singleSourceShortestPaths.print()

    singleSourceShortestPaths.collect().toSeq.map(f => Tuple2[Integer, Integer](f.getId, f.getValue))
  }

  // --------------------------------------------------------------------------------------------
  //  Single Source Shortest Path UDFs
  // --------------------------------------------------------------------------------------------

  private final class MinDistanceMessenger extends ScatterFunction[Integer, Integer, Integer, Integer] {

    override def sendMessages(vertex: Vertex[Integer, Integer]) {
      if (vertex.getValue < Integer.MAX_VALUE)
        for (edge: Edge[Integer, Integer] <- getEdges) {
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
      var minDistance = Integer.MAX_VALUE
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

  // dynamic
  def runDynamic(graph: Graph[Integer, Integer, Integer], addedEdges: Seq[Edge[Integer, Integer]]) = {
    var modifiedGraph = graph
    val srcId = 1

    modifiedGraph = graph.mapVertices(v => if (v.getId.equals(srcId)) {
      0
    } else {
      Integer.MAX_VALUE
    })

    val a =modifiedGraph.getVertices.count()
    val b = modifiedGraph.getVertices.filter(v => v.getValue == Integer.MAX_VALUE).count()

    val maxIterations = 5

    if(b == a-1){
      modifiedGraph = modifiedGraph.runScatterGatherIteration(new MinDistanceMessenger, new VertexDistanceUpdater, maxIterations)
    }


    modifiedGraph = modifiedGraph.runScatterGatherIteration(new RecalculateMessenger(addedEdges), new VertexDistanceUpdater, maxIterations)

    modifiedGraph.getVertices.print()


    modifiedGraph.getVertices.collect().toSeq.map(f => Tuple2[Integer, Integer](f.getId, f.getValue))
  }


  private final class RecalculateMessenger(var edgesToBeChanged: Seq[Edge[Integer, Integer]]) extends ScatterFunction[Integer, Integer, Integer, Integer] {
    override def sendMessages(vertex: Vertex[Integer, Integer]) {
      if (vertex.getValue < Integer.MAX_VALUE)
        for(edge <- edgesToBeChanged)
          if (vertex.getId.equals(edge.getSource)){
            sendMessageTo(edge.getTarget, vertex.getValue + edge.getValue)
          }
    }
  }

}


