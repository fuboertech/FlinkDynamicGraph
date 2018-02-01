package org.ilmenau.groupstudy.flinkdynamicgraph.generator


import java.util.concurrent.TimeUnit

import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.ilmenau.groupstudy.flinkdynamicgraph.graph.AbstractGraph
import org.ilmenau.groupstudy.flinkdynamicgraph.loader.DataLoader
import org.ilmenau.groupstudy.flinkdynamicgraph.model.Route

import scala.util.Random

object DataGenerator {

  private var _graph: AbstractGraph = _
  private val _airportsSize = DataLoader.airports.count().toInt
  private val _airlinesSize = DataLoader.airlines.count().toInt
  private val _airports = DataLoader.airports.map(a => a.airportID).collect()
  private val _airlines = DataLoader.airlines.map(a => a.airlineID).collect()
  private val NewRouteRecordMaxDelay: Int = 300

  /** Generate new routes for the graph.
    * Every period of time routes between existing airports will be generated and inserted into graph */
  def generate(env: StreamExecutionEnvironment, graph: AbstractGraph, timeWindowLength: Int): Unit = {
    _graph = graph

    val input = env.addSource(generateRoutes _)
    input.timeWindowAll(Time.of(timeWindowLength, TimeUnit.MILLISECONDS)).apply(new GraphModelBuilder)

    // TODO:  assync version. check on cluster
//    val asyncMapped = AsyncDataStream.orderedWait(input, 10000L, TimeUnit.MILLISECONDS, 10) {
//      (input, collector: AsyncCollector[Route]) =>
//        Future {
//          collector.collect(Seq(input))
//        } (ExecutionContext.global)
//    }
//    asyncMapped.timeWindowAll(Time.of(5000, TimeUnit.MILLISECONDS)).apply(new GraphModelBuilder)
  }

  /** Builds up-to-date model of the graph with generated routes */
  private class GraphModelBuilder extends AllWindowFunction[Route, Array[Route], TimeWindow] {

    protected def buildGraphModel(values: Iterable[Route]): Array[Route] = {
      _graph.addEdges(values)
      Array[Route]()
    }

    override def apply(window: TimeWindow,
                       values: Iterable[Route],
                       out: Collector[Array[Route]]): Unit = {
      out.collect(buildGraphModel(values))
    }
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
      Thread.sleep(Random.nextInt(NewRouteRecordMaxDelay))
    }
  }

}
