package org.ilmenau.groupstudy.flinkdynamicgraph.app

import org.ilmenau.groupstudy.flinkdynamicgraph.example.WordCount


object App {

  def main(args: Array[String]): Unit = {
    // run example
    WordCount.count(args)
  }
}
