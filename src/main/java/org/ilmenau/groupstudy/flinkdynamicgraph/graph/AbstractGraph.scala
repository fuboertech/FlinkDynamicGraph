package org.ilmenau.groupstudy.flinkdynamicgraph.graph

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.graph.scala.Graph
import org.ilmenau.groupstudy.flinkdynamicgraph.model.{Airport, Route}

abstract class AbstractGraph(var env: ExecutionEnvironment) {

  protected var graph: Graph[Integer, Airport, Integer] = _

  def addEdges(routes: Iterable[Route]): Unit

  def get: Graph[Integer, Airport, Integer] = graph

}
