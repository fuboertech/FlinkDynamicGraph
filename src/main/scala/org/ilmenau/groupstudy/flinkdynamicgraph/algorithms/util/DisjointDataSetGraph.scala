package org.ilmenau.groupstudy.flinkdynamicgraph.algorithms.util

import scala.collection.mutable
import scala.reflect.ClassTag

class DisjointDataSetGraph[K >: Null : ClassTag](val matches: mutable.HashMap[K, K] = mutable.HashMap.empty) {

  private val ranks: mutable.HashMap[K, Integer] = mutable.HashMap.empty

  def getMatches: mutable.HashMap[K, K] = matches

  def makeSet(vertexId: K): Unit ={
    matches.put(vertexId, vertexId)
    ranks.put(vertexId, 0)
  }

  /**
    * Union combines the two possibly disjoint sets where e1 and e2 belong in.
    * Optimizations:
    * <p/>
    * - In case e1 or e2 do not exist they are being added directly in the same disjoint set.
    * - Union by Rank to minimize lookup depth
    *
    * @param e1
    * @param e2
    */
  def union(e1: K, e2: K):Unit ={
    if (!matches.contains(e1)) {
      makeSet(e1)
    }
    if (!matches.contains(e2)) {
      makeSet(e2)
    }

    var root1 = find(e1).orNull
    var root2 = find(e2).orNull

    if (root1.equals(root2)) {
      return
    }

    var dist1 = ranks(root1)
    var dist2 = ranks(root2)
    if (dist1 > dist2) {
      matches.put(root2, root1)
    } else if (dist1 < dist2) {
      matches.put(root1, root2)
    } else {
      matches.put(root2, root1)
      ranks.put(root1, dist1 + 1)
    }
  }

  /**
    * Find returns the root of the disjoint set e belongs in.
    * It implements path compression, flattening the tree whenever used, attaching nodes directly to the disjoint
    * set root if not already.
    *
    * @param vertexId
    * @return the root of the connected component
    */
  def find( vertexId: K): Option[K] = {
    if (!matches.contains(vertexId)) {
      return None
    }

    var parent: K = matches(vertexId)
    if (!parent.equals(vertexId)) {
      var tmp = find(parent).orNull
      if (!parent.equals(tmp)) {
        parent = tmp
        matches.put(vertexId, parent)
      }
    }
    Some(parent)
  }

  /**
    * Merge works in a similar fashion to a naive symmetric hash join.
    * We keep the current disjoint sets and attach all nodes of 'other' incrementally
    * There is certainly room for further optimisations...
    *
    * @param other
    */
  def merge(other: DisjointDataSetGraph[K]) {
    other.getMatches.foreach(entry => union(entry._1, entry._2))
  }
}

