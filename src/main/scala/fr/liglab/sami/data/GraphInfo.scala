package fr.liglab.sami.data

import fr.liglab.sami.code.DAG
import fr.liglab.sami.code.Edge
import net.openhft.koloboke.collect.set.IntSet

trait GraphInfo {
  def getExtensions(leaves: IntSet, leafLabel: Int, minSupport: Int): Iterable[(Edge, DAG)]
  def getExtensions(e: Edge): DAG
  def getGraph: Iterable[(Edge, DAG)]
  def getGraphMap: collection.Map[Edge, DAG]
}

////otherV,iLabel,edgeLabel,jLabel,out
//class IgniteGraph(c: IgniteCache[Int, List[(Int, Int, Int, Int, Byte)]]) extends GraphInfo {
//  override def getExtensions(leaves: Set[Int], minSupport: Int): Iterable[((Int, Int, Byte), List[(Int, Int, Int)])] = {
//    val cache_result = c.getAll(leaves)
//    cache_result.toList.flatMap { case (k, v) => v.map(edgeTuple => ((edgeTuple._3, edgeTuple._4, edgeTuple._5), (k, edgeTuple._1, edgeTuple._2))) }
//      .groupBy(_._1) //group edges that have the same edgeLabel,jLabel and direction...
//      .filter(_._2.size >= minSupport) //early support check
//      .toMap
//      .mapValues(_.map(_._2))
//  }
//}
//
//class BasicGraph(g: Map[Int, List[IncidentEdge]]) extends GraphInfo {
//  override def getExtensions(leaves: Set[Int], minSupport: Int): Iterable[((Int, Int, Byte), List[(Int, Int, Int)])] = {
//    leaves.toList.map(l => (l, g(l))).flatMap { case (k, v) => v.map(edge => ((edge.edgeLabel, edge.jLabel, edge.direction), (k, edge.otherV, edge.iLabel))) }
//      .groupBy(_._1) //group edges that have the same edgeLabel,jLabel and direction...
//      .filter(_._2.size >= minSupport) //early support check
//      .toMap
//      .mapValues(_.map(_._2))
//  }
//}

class PreGroupedGraph(val m: collection.Map[Edge, DAG]) extends GraphInfo {
  override def getExtensions(leaves: IntSet, leafLabel: Int, minSupport: Int): Iterable[(Edge, DAG)] = {
    def isFreq(dag: DAG): Boolean = {
      var leavesLeft = leaves.size
      var intersectSize = 0
      val cursor = leaves.cursor()
      while (cursor.moveNext()) {
        if (dag.hasEdgeFrom(cursor.elem())) {
          intersectSize += 1
        }
        leavesLeft -= 1
        if (intersectSize == minSupport) return true
        if (intersectSize + leavesLeft < minSupport) return false
      }
      return false
    }
    return m.filter(p => p._1.iLabel == leafLabel && isFreq(p._2))
  }

  override def getExtensions(e: Edge): DAG = {
    return m(e)
  }

  override def getGraph: Iterable[(Edge, DAG)] = getGraphMap
  override def getGraphMap: collection.Map[Edge, DAG] = m

}

object GraphInfo {
  var graphReader: GraphInfo = null
}