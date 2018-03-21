package org.ilmenau.groupstudy.flinkdynamicgraph.algorithms.util

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.java.{DataSet => JavaDataSet}
import org.apache.flink.api.scala._
import org.apache.flink.graph.library.GSAConnectedComponents
import org.apache.flink.graph.scala.Graph
import org.apache.flink.graph.utils.GraphUtils.IdentityMapper
import org.apache.flink.graph.{Edge, Vertex}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.types.NullValue
import org.apache.flink.util.Collector
import org.ilmenau.groupstudy.flinkdynamicgraph.loader.DataLoader

import scala.collection.mutable

object ConnectedComponentsAlgorithmDebug {

  def main (args: Array[String] ): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    DataLoader.load(env)

    val edges: DataSet[Edge[Integer, NullValue]] = DataLoader.testCC.map(j => new Edge(j.sourceAirportID, j.destAirportID, NullValue.getInstance())).filter(e => (!e.getSource.equals(-1) && !e.getTarget.equals(-1)))

    val disjointDataSet = new DisjointDataSet[Integer](mutable.HashMap.empty[Integer, Integer])
    val vert = edges.flatMap((e: Edge[Integer, NullValue], out: Collector[Integer]) => {
      out.collect(e.getSource)
      out.collect(e.getTarget)
    }).distinct()
    val graph: Graph[Integer, Integer, NullValue] = Graph.fromDataSet(edges, new IdentityMapper[Integer](), env)
    graph.getEdges.print()
    val r = graph.run(new GSAConnectedComponents[Integer, Integer, NullValue](99)).asInstanceOf[org.apache.flink.api.java.DataSet[Vertex[Integer, NullValue]]]
//    r.map(v => (v.getId, v.getValue.toString)).groupBy(1).reduceGroup {
//      (in, out: Collector[(Integer, String)]) =>
//        in.forEach()
//    }
    //vert.print()
    vert.collect().foreach(e => disjointDataSet.makeSet(e))
    edges.collect().foreach(e => disjointDataSet.union(e.getSource, e.getTarget))

    disjointDataSet.getMatches.foreach(println)
    //disjointDataSet.ranks.foreach(println)

    //var result = ConnectedComponentsAlgorithm.runDynamic(null, edges.collect().toSeq)
  }

  def runDynamic(env: StreamExecutionEnvironment,
                 addedEdges: Seq[Edge[Integer, Integer]],
                 components: mutable.HashMap[Integer, mutable.HashSet[Integer]] = mutable.HashMap.empty[Integer, mutable.HashSet[Integer]]):
  mutable.HashMap[Integer, mutable.HashSet[Integer]] = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val edges: DataStream[Edge[Integer, Integer]] = env.fromCollection(addedEdges)
    val assignComponents = new AssignComponents

    val result: DataStream[Edge[Integer, Integer]] = edges.iterate(
      iteration => {
        val res = iteration.keyBy(0).flatMap(assignComponents).setParallelism(1)
        (res, res)
      }, 10000, false
    )

    // Emit the results
//    result.map(e => (e.getSource, e.getTarget)).
//      writeAsCsv("iter")
    println("here")
    val res = env.execute("Streaming Connected Components")
    println("here2")
    CollectSink.values.foreach(println)
    CollectSink.values
  }
}

// create a testing sink
object CollectSink  {

  val values = mutable.HashMap.empty[Integer, mutable.HashSet[Integer]]

//  def put(source: Integer, target: Integer): Unit = {
//    val maybeIntegers = values.get(source)
//    if (maybeIntegers.isEmpty) {
//      val set = ParHashSet[Integer](target)
//      values.put(source, set)
//    } else {
//      maybeIntegers.get += target
//    }
//  }
}


class AssignComponents
  extends RichFlatMapFunction[Edge[Integer, Integer], Edge[Integer, Integer]] {

  //def setComponents(components: mutable.HashMap[Integer, mutable.HashSet[Integer]]) = {CollectSink2.values}

  def flatMap(edge: Edge[Integer, Integer], out: Collector[Edge[Integer, Integer]]) {
    //println("here3 " + edge.getSource + " " + edge.getTarget)
    val sourceId = edge.getSource
    val targetId = edge.getTarget
    var sourceComp = -1
    var trgComp = -1

    // check if the endpoints belong to existing components
    CollectSink.values.foreach(entry => {
      //println(entry)
      if ((sourceComp == -1) || (trgComp == -1)) {
        if (entry._2.contains(sourceId)) sourceComp = entry._1
        if (entry._2.contains(targetId)) trgComp = entry._1
      }
    })
    if (sourceComp != -1) {
      // the source belongs to an existing component
      if (trgComp != -1) {
        // merge the components
        merge(sourceComp, trgComp, out)
      }
      else {
        // add the target to the source's component
        // and update the component Id if needed
        addToExistingComponent(sourceComp, targetId, out)
      }
    }
    else {
      // the source doesn't belong to any component
      if (trgComp != -1) {
        // add the source to the target's component
        // and update the component Id if needed
        addToExistingComponent(trgComp, sourceId, out)
      }
      else {
        // neither src nor trg belong to any component
        // create a new component and add them in it
        createNewComponent(sourceId, targetId, out)
      }
    }
  }

  def createNewComponent(sourceId: Integer, targetId: Integer, out: Collector[Edge[Integer, Integer]]) {
    var componentId = Math.min(sourceId, targetId)
    val vertexSet: mutable.HashSet[Integer] = mutable.HashSet.empty
    vertexSet+=sourceId
    vertexSet+=targetId
    CollectSink.values.put(componentId, vertexSet)
    out.collect(new Edge(sourceId, componentId, 0))
    out.collect(new Edge(targetId, componentId, 0))
  }

  def addToExistingComponent(componentId: Integer, toAdd: Integer, out: Collector[Edge[Integer, Integer]]) {
    //println("here323")
    val vertices = CollectSink.values.remove(componentId).get

    if (componentId >= toAdd) {
      // output and update component ID
      vertices.foreach(v => out.collect(new Edge(v, toAdd, 0)))
      vertices+=toAdd
      CollectSink.values.put(toAdd, vertices)
    }
    else {
      CollectSink.values.put(componentId, vertices)
      println(toAdd, componentId)
      out.collect(new Edge(toAdd, componentId, 0))
    }
  }

  def merge(sourceComp: Integer, trgComp: Integer, out: Collector[Edge[Integer, Integer]]) {
    val srcVertexSet = CollectSink.values.remove(sourceComp).orNull
    val trgVertexSet = CollectSink.values.remove(trgComp).orNull
    var componentId = Math.min(sourceComp, trgComp)
    if (sourceComp == componentId) {
      // collect the trgVertexSet
      if (trgVertexSet != null) {
        trgVertexSet.foreach(v => out.collect(new Edge(v, componentId, 0)))
      }
    }

    else {
      // collect the srcVertexSet
      if (srcVertexSet != null) {
        srcVertexSet.foreach(v => out.collect(new Edge(v, componentId, 0)))
      }
    }
    if (trgVertexSet != null) {
      trgVertexSet.foreach(t => srcVertexSet+=t)
    }
    CollectSink.values.put(componentId, srcVertexSet)
  }

}

