package org.ilmenau.groupstudy.flinkdynamicgraph.model

case class ChangesModel[K, V](added: ChangeModel[K, V], deleted: ChangeModel[K,V])

case class ChangeModel[K, V](edges: K, vertices: V)