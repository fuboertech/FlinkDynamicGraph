package org.ilmenau.groupstudy.flinkdynamicgraph.generator


import java.util.concurrent.TimeUnit

import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.ilmenau.groupstudy.flinkdynamicgraph.algorithms.{ConnectedComponentsAlgorithm, PageRankAlgorithm}
import org.ilmenau.groupstudy.flinkdynamicgraph.graph.AbstractGraph
import org.ilmenau.groupstudy.flinkdynamicgraph.loader.DataLoader
import org.ilmenau.groupstudy.flinkdynamicgraph.model.data.Route

import scala.util.Random

object DataGenerator {

  private var _graph: AbstractGraph = _
  private val _airportsSize = DataLoader.airports.count().toInt
  private val _airlinesSize = DataLoader.airlines.count().toInt
  private val _airports = DataLoader.airports.map(a => a.airportID).collect()
  private val _airlines = DataLoader.airlines.map(a => a.airlineID).collect()
  private val NewRouteRecordMaxDelay: Int = 300

  object GraphChangesApplier extends AllWindowFunction[Route, Array[Route], TimeWindow]{

    var pagerankAlg: PageRankAlgorithm = _

   // val connectedComponentsAlg: ConnectedComponentsAlgorithm = ConnectedComponentsAlgorithm

    protected def changeGraphModel(values: Iterable[Route]): Array[Route] = {
      if (pagerankAlg == null) {
        pagerankAlg = new PageRankAlgorithm()
        pagerankAlg.runClassic(_graph.get)
        println("===========")
      }

      var addedEdges = _graph.addEdges(values)


      val classicPageRank = pagerankAlg.runClassic(_graph.get)
      println("\n\n\n+++ classic PageRank size: " + classicPageRank.size)

      val dynamicPageRank = pagerankAlg.runDynamic(_graph.get, addedEdges, _graph.env).toSeq
      println("\n\n\n+++ dynamic PageRank size: " + dynamicPageRank.size)

      val classicConnectedComponents = ConnectedComponentsAlgorithm.runClassic(_graph.get)
      println("\n\n\n+++ classic ConnectedComponents size: " + classicConnectedComponents.size)

      val dynamicConnectedComponents = ConnectedComponentsAlgorithm.runDynamic(_graph.get, addedEdges)
      println("\n\n\n+++ dynamic ConnectedComponents size: " + dynamicConnectedComponents.size)


      Array[Route]()
    }

    override def apply(window: TimeWindow,
                       values: Iterable[Route],
                       out: Collector[Array[Route]]): Unit = {
      out.collect(changeGraphModel(values))
    }
  }

  /** Generate new routes for the graph.
    * Every period of time routes between existing airports will be generated and inserted into graph */
  def generate(env: StreamExecutionEnvironment,
               graph: AbstractGraph,
               timeWindowLength: Int): Unit = {
    _graph = graph

    val input = env.addSource(generateRoutes _)
    input.timeWindowAll(Time.of(timeWindowLength, TimeUnit.MILLISECONDS)).apply(GraphChangesApplier)

    // TODO:  assync version. check on cluster
//    val asyncMapped = AsyncDataStream.orderedWait(input, 10000L, TimeUnit.MILLISECONDS, 10) {
//      (input, collector: AsyncCollector[Route]) =>
//        Future {
//          collector.collect(Seq(input))
//        } (ExecutionContext.global)
//    }
//    asyncMapped.timeWindowAll(Time.of(5000, TimeUnit.MILLISECONDS)).apply(new GraphModelBuilder)
  }

  private def generateRoutes(ctx: SourceContext[Route]): Unit = {
    var i = 9999
    while (true) {
      ctx.collect(Route(i.toString,
        _airlines.apply(Random.nextInt(_airlinesSize)),
        Random.nextInt(100).toString,
        _airports.apply(Random.nextInt(_airportsSize)),
        Random.nextInt(100).toString,
        _airports.apply(Random.nextInt(_airportsSize)),
        "",
        Random.nextInt(10),
        "AAA BBB"
      ))
      i+=1
      Thread.sleep(Random.nextInt(NewRouteRecordMaxDelay/10))
    }
  }

}
