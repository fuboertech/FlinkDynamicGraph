package org.ilmenau.groupstudy.flinkdynamicgraph.graph

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.graph.Edge
import org.apache.flink.graph.scala.Graph
import org.ilmenau.groupstudy.flinkdynamicgraph.model.data.{Airport, Route}

abstract class AbstractGraph(var env: ExecutionEnvironment) {

<<<<<<< HEAD
  protected var graph: Graph[Integer, Double, Integer] = _
=======
  protected var graph: Graph[Integer, Integer, Integer] = _
>>>>>>> origin/master

  def addEdges(routes: Iterable[Route]): Seq[Edge[Integer, Integer]]

<<<<<<< HEAD
  def get: Graph[Integer, Double, Integer] = graph
=======
  def get: Graph[Integer, Integer, Integer] = graph
>>>>>>> origin/master

}
