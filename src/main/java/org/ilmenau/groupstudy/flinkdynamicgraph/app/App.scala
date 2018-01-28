package org.ilmenau.groupstudy.flinkdynamicgraph.app

import org.ilmenau.groupstudy.flinkdynamicgraph.example.WordCount
import org.ilmenau.groupstudy.flinkdynamicgraph.loader.DataLoader


object App {

  def main(args: Array[String]): Unit = {
    // run example
    // WordCount.count(args)

    // load databases
    DataLoader.load()

    print(DataLoader.routes.count())
  }
}
