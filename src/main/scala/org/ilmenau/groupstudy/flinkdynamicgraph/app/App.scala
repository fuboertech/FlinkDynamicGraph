package org.ilmenau.groupstudy.flinkdynamicgraph.app

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.ilmenau.groupstudy.flinkdynamicgraph.generator.DataGenerator
import org.ilmenau.groupstudy.flinkdynamicgraph.graph.AirlinesGraph
import org.ilmenau.groupstudy.flinkdynamicgraph.loader.DataLoader


object App {

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    // load databases
    DataLoader.load(env)

    // create graph
    val graph = new AirlinesGraph(env)
    graph.construct()
    println(graph.get.getVertices.count())

    // start process of changing graph by adding new routes every 5 seconds and finding PageRank
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment
    DataGenerator.generate(streamEnv, graph, 5000)
    streamEnv.execute("Routes stream")
  }

}
