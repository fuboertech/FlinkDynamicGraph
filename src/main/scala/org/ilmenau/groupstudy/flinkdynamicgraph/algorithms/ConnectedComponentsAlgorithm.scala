package org.ilmenau.groupstudy.flinkdynamicgraph.algorithms

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala._
import org.apache.flink.graph.{Edge, Vertex}
import org.apache.flink.graph.scala.Graph
import org.apache.flink.types.{DoubleValue, LongValue, NullValue}
import org.ilmenau.groupstudy.flinkdynamicgraph.model.data.Airport
import org.apache.flink.api.java.{Utils, DataSet => JavaDataSet}
import org.apache.flink.configuration.Configuration
import org.apache.flink.graph.library.{ConnectedComponents, GSAConnectedComponents}
import org.apache.flink.graph.utils.GraphUtils.IdentityMapper
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector
import org.ilmenau.groupstudy.flinkdynamicgraph.algorithms.util.DisjointDataSet
import org.ilmenau.groupstudy.flinkdynamicgraph.loader.DataLoader

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.parallel.mutable.{ParHashMap, ParHashSet}

object ConnectedComponentsAlgorithm {

  private var _disjointSet: DisjointDataSet[Integer] = _


  def runClassic(graph: Graph[Integer, Integer, Integer]): Seq[(Integer, Integer)] = {
    val result = graph.run(new GSAConnectedComponents[Integer, Integer, Integer](100)).collect().asScala.toSeq.map(v => (v.getId, v.getValue))
    result
  }

  def runDynamic(graph: Graph[Integer, Integer, Integer], addedEdges: Seq[Edge[Integer, Integer]]): Seq[(Integer, Integer)] = {
    if (_disjointSet == null) {
      _disjointSet = new DisjointDataSet[Integer](mutable.HashMap.empty[Integer, Integer])
      graph.getVertices.collect().foreach(e => _disjointSet.makeSet(e.getId))
    }
    addedEdges.foreach(e => _disjointSet.union(e.getSource, e.getTarget))
    _disjointSet.getMatches.toSeq
  }
}
