package org.ilmenau.groupstudy.flinkdynamicgraph.app

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.graph._
import org.apache.flink.graph.library.link_analysis.PageRank
import org.apache.flink.graph.scala.Graph
import org.apache.flink.streaming.api.scala._
import org.ilmenau.groupstudy.flinkdynamicgraph.graph.AirportsAirlinesGraph
import org.ilmenau.groupstudy.flinkdynamicgraph.model.{Airline, Airport, Route}


object App {

  def main(args: Array[String]): Unit = {
    // run example
    // WordCount.count(args)

    // create graph
    AirportsAirlinesGraph.construct()

    // run PageRank algorithm
    AirportsAirlinesGraph.get.run(new PageRank[Integer, Airport, (Route, Airline)](0.85,  0.01)).print()
  }
}
