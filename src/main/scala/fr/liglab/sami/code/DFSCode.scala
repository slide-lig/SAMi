package fr.liglab.sami.code

import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable.ListBuffer

import fi.tkk.ics.jbliss.Graph
import fi.tkk.ics.jbliss.Reporter
import fr.liglab.sami.utilities.AutomorphReporter

class DFSCode(private val code: Seq[Edge], private val vertexCount: Int) extends Serializable with Equals with Ordered[DFSCode] {
  // vertices labeled from 0 to vertexCount-1, 0 is the root and vertexCount-1 the rightmost vertex
  // path from root to rightmost vertex is rightmost path
  // nb forward edges = vertexCount-1, as each forward edge must connect to a new node (part of the DFS tree)
  private var isCanonicalResult: Boolean = false
  private var wasCanonicalTested: Boolean = false
  //  if(this.code.size != 0 && this.code(0).iLabel == 31) println("creating " + this)

  def isCanonical: Boolean = this.testCanonical()

  def codeLength(): Int = {
    this.code.length
  }

  def getCFLRepresentation: String = {
    val sb = new StringBuilder("t " + DFSCode.CFLIndexGen.getAndIncrement + " " + this.vertexCount + " " + (this.code.length * 2) + "\n")
    val g = genGraph
    for (i <- 0 until this.vertexCount) {
      sb.append(i + " " + g(i).head.iLabel + " " + g(i).size)
      for (e <- g(i).map(_.j).sorted) {
        sb.append(" " + e)
      }
      sb.append("\n")
    }
    return sb.toString
  }

  def requiresAutomorphismVerif: Boolean = {
    return this.code.exists(e => e.direction != 0 || e.edgeLabel != 0)
  }

  def getAutomorphismMNITransfers: Array[List[Int]] = {
//    val res: Array[List[Int]] = Array.fill(vertexCount)(Nil)
//    for (i <- 0 until res.size) {
//      res(i) = List(i)
//    }
//    return res
    val blissG: Graph[Integer] = new Graph;
    var first = true
    for (e <- code) {
      if (first) {
        first = false
        blissG.add_vertex(0, e.iLabel + 1)
      }
      if (e.j > e.i) {
        blissG.add_vertex(e.j, e.jLabel + 1)
      }
      blissG.add_edge(e.j, e.i)
    }
    //    blissG.write_dot(System.out)
    val reporter = new AutomorphReporter(this)
    blissG.find_automorphisms(reporter, null)
    val mappings = reporter.getMappings
    val res: Array[List[Int]] = Array.fill(vertexCount)(Nil)
    for (i <- mappings.size - 1 to 0 by -1) {
      res(mappings(i)) = i :: res(mappings(i))
    }
//        println(this + " automorph detected " + res.mkString(" - "))
    return res
  }

  def getAncestors(): List[DFSCode] = {
    if (this.codeLength() < 2)
      return List[DFSCode](this)

    val e1 = this.getEdgeAt(this.codeLength() - 2)
    val e2 = this.getEdgeAt(this.codeLength() - 1)
    val prefix = this.getPrefix(this.codeLength() - 2)

    // extension
    if (e1.isForwardEdge() && e2.isForwardEdge() && e1.j == e2.i)
      return this.getPrefix(this.codeLength() - 1).getAncestors() ++ List[DFSCode](this)

    // intersection FF
    if (e1.isForwardEdge() && e2.isForwardEdge()) {
      val p1 = prefix.extend(e1)
      val p2 = prefix.extend(e2.unshiftFF)
      return p1.getAncestors() ++ p2.getAncestors() ++ List[DFSCode](this)
    }

    // intersection FB
    if (e1.isForwardEdge() && !e2.isForwardEdge()) {
      if (e1.i != e2.i)
        throw new Exception("unexpected FB code branch")
      val p1 = prefix.extend(e1)
      val p2 = prefix.extend(e2.unshiftFB)
      return p1.getAncestors() ++ p2.getAncestors() ++ List[DFSCode](this)
    }

    //intersection BF or BB
    val p1 = prefix.extend(e1)
    val p2 = prefix.extend(e2)
    return p1.getAncestors() ++ p2.getAncestors() ++ List[DFSCode](this)
  }

  def getPrefix(len: Int): DFSCode = {
    if (this.code.length < len) {
      throw new IllegalArgumentException("requested prefix length too long")
    }
    var result = new DFSCode(Vector(), 0)
    if (len >= 1)
      result = new DFSCode(Vector(new Edge(this.code(0))), 2)
    for (i <- 1 until len) {
      result = result.extend(new Edge(this.code(i)))
    }
    return result
  }
  def getEdgeAt(i: Int): Edge = {
    return this.code(i)
  }
  def getSuffix(len: Int): DFSCode = {
    if (this.code.length < len) {
      throw new IllegalArgumentException("requested suffix length too long")
    }
    val offset = this.code.length - len
    var result = new DFSCode(Vector(new Edge(this.code(offset))), 2)
    for (i <- 1 until len) {
      result = result.extend(new Edge(this.code(i + offset)))
    }
    return result
  }
  def concat(otherCode: DFSCode): DFSCode = {
    val offset = this.code.length
    var result = new DFSCode(this.code, this.vertexCount)
    for (i <- 0 until otherCode.codeLength()) {
      result = result.extend(new Edge(otherCode.getEdgeAt(i)))
    }
    return result
  }
  override def compare(other: DFSCode): Int = {
    val diffSize = this.codeLength() - other.codeLength()
    if (diffSize != 0) return -diffSize
    for (i <- 0 until other.codeLength()) {
      if (i >= this.code.length) { return 1 }
      val diff = this.code(i).compare(other.getEdgeAt(i))
      if (diff != 0) { return diff }
    }
    return 0
  }

  override def equals(that: Any) =
    that match {
      case that: DFSCode => that.canEqual(this) && that.code.equals(this.code)
      case _             => false
    }

  def canEqual(other: Any) = other.isInstanceOf[DFSCode]

  def getLabelForId(id: Int, edges: Seq[Edge]): Int = {
    for (e <- edges) {
      if (e.i == id)
        return e.iLabel
      if (e.j == id)
        return e.jLabel
    }
    return -1
  }

  def extend(e: Edge): DFSCode = {
    // label sanity checks
    if (this.code.size > 0) {
      val iLbl = getLabelForId(e.i, this.code)
      val jLbl = getLabelForId(e.j, this.code)
      if (e.isForwardEdge() && (iLbl == -1 || iLbl != e.iLabel))
        return null
      if (!e.isForwardEdge() && (iLbl == -1 || iLbl != e.iLabel || jLbl == -1 || jLbl != e.jLabel))
        return null
      if (e.isForwardEdge() && e.j < this.vertexCount) {
        throw new RuntimeException("impossible code " + this + " extended with " + e)
      }
    }
    val edges: Seq[Edge] = this.code :+ e
    if (e.isForwardEdge()) {
      if (this.code.length == 0)
        return new DFSCode(edges, this.vertexCount + 2)
      else
        return new DFSCode(edges, this.vertexCount + 1)
    } else {
      if (this.code.length == 0)
        throw new Exception("Unexpected backward-edge starting a DFSCode")
      else
        return new DFSCode(edges, this.vertexCount)
    }
  }

  def getEdgesRelatedTo(node: Int): List[Edge] = {
    this.code.filter(e => e.i == node || e.j == node).toList
  }

  def canAcceptExtensionBackEdge(e: Edge): Boolean = {
    val lastEdge: Edge = this.code(this.code.size - 1)
    if (lastEdge.isForwardEdge()) {
      return lastEdge.j == e.i && (Edge.equalAndOppositeDir(lastEdge, e) || Edge.notEqualAndAnyDir(lastEdge, e))
    } else {
      val prevForwardEdge = this.getPrevForwardEdge()
      return lastEdge.compareTo(e) < 0 && prevForwardEdge.j == e.i && (Edge.equalAndOppositeDir(prevForwardEdge, e) || Edge.notEqualAndAnyDir(prevForwardEdge, e))
    }
  }

  def getPrevForwardEdge(): Edge = {
    for (i <- this.code.size - 1 to 0 by -1) {
      if (this.code(i).isForwardEdge()) {
        return this.code(i)
      }
    }
    throw new Exception("No forward edge present in DFSCode")
    return null
  }
  def getLastBackEdgeDest(): Int = {
    val lastEdge: Edge = this.code(this.code.size - 1)
    if (lastEdge.isForwardEdge()) {
      return -1
    } else {
      return lastEdge.j
    }
  }

  // def containsExtensionBackEdge(e: Edge): Boolean = {
  //   for (i <- this.code.size - 1 to 0 by -1) {
  //     if (this.code(i).i != e.i) {
  //       return this.code(i).j == e.i && (this.code(i).label == e.label && this.code(i).fromItoJ != e.fromItoJ)
  //     } else if (this.code(i).equals(e)) {
  //       return true
  //     }
  //   }
  //   return false
  // }

  def hasPotential: Boolean = {
    if (this.code.size < 2) {
      true
    } else {
      this.hasPotential(this.code(this.code.length - 1))
    }
  }

  def hasPotential(e: Edge): Boolean = {
    this.code(0).compare(e.asCodeStart) <= 0
  }

  override def hashCode: Int = {
    val prime = 31
    var result = 1
    for (edge: Edge <- this.code) {
      result = prime * result + edge.hashCode()
    }
    return result
  }

  def getRightPath(): List[Int] = {
    var prevParent = Int.MaxValue
    var rightPath: List[Int] = Nil
    for (i <- (code.size - 1) to 0 by -1) {
      val e: Edge = this.code(i)
      if (e.isForwardEdge() && e.i < prevParent) {
        prevParent = e.i
        rightPath = e.j :: rightPath
      }
    }
    return 0 :: rightPath
  }

  def isOnRightmost(v: Int): Boolean = {
    if (v == 0) return true
    var prevParent = Int.MaxValue
    for (i <- (code.size - 1) to 0 by -1) {
      val e: Edge = this.code(i)
      if (e.isForwardEdge() && e.i < prevParent) {
        prevParent = e.i
        if (e.j == v) return true
      }
    }
    return false
  }

  def getNbNodes(): Int = {
    vertexCount
  }

  def getRightmostNode(): Int = {
    vertexCount - 1
  }

  def testCanonical(): Boolean = {
    if (this.wasCanonicalTested) {
      return this.isCanonicalResult
    }
    this.wasCanonicalTested = true
    //create graph structure to start enumerating DFS codes
    val graph = genGraph()
    //    graph.foreach(println)
    //now we need to iterate DFS trees and compare the codes
    val mappings: Array[Int] = Array.fill(this.vertexCount)(-1)
    for (i <- 0 until this.vertexCount) {
      mappings(i) = 0
      val assignable = graph(i).map(_.j).toSet
      if (this.recNonCanonical(List(assignable), mappings, graph, 1, 0)) {
        this.isCanonicalResult = false
        return false
      }
      mappings(i) = -1
    }
    this.isCanonicalResult = true
    return true
  }

  private def genGraph(): Array[List[Edge]] = {
    val graph: Array[List[Edge]] = Array.fill(this.vertexCount)(Nil)
    for (e: Edge <- code) {
      graph(e.i) = e :: graph(e.i)
      graph(e.j) = e.shiftFB :: graph(e.j)
    }
    return graph
  }

  def checkValidAutomorphism(permutation: Array[Int]): Boolean = {
    val graph = genGraph.map(_.toSet)
    for (i <- 0 until permutation.size) {
      //      e.i, e.j, e.iLabel, e.edgeLabel, e.jLabel, e.direction)
      if (permutation(i) != i) {
        val permuted = graph(i).map(e => Edge.makeEdge(permutation(e.i), permutation(e.j), e.iLabel, e.edgeLabel, e.jLabel, e.direction))
        if (!permuted.equals(graph(permutation(i)))) return false
      }
    }
    return true
  }

  //returns true if a better code is found
  private def recNonCanonical(assignable: List[Set[Int]], mappings: Array[Int], graph: Array[List[Edge]], nextId: Int, nbEdgesDone: Int): Boolean = {
    //    println("mappings " + mappings.mkString(","))
    //    println("assignable " + assignable)
    var cleanedAssignable = assignable
    while (!cleanedAssignable.isEmpty && !cleanedAssignable.head.exists(mappings(_) == -1)) {
      cleanedAssignable = cleanedAssignable.tail
    }
    if (cleanedAssignable.isEmpty) {
      return false
    } else {
      val choices = cleanedAssignable.head.filter(mappings(_) == -1)
      //      println("choices " + choices)
      for (assigned <- choices) {
        var badChoice = false
        //        println("trying " + assigned)
        var edgesToAssignedNodes: ListBuffer[Edge] = new ListBuffer()
        val futureExtensions: ListBuffer[Edge] = new ListBuffer()
        for (e <- graph(assigned)) {
          if (mappings(e.j) != -1) {
            edgesToAssignedNodes += e
          } else {
            futureExtensions += e
          }
        }
        edgesToAssignedNodes = edgesToAssignedNodes.sortWith(
          //sort order of forward edges
          (x, y) =>
            mappings(x.j) > mappings(y.j) ||
              (mappings(x.j) == mappings(y.j) && x.direction < y.direction) ||
              (mappings(x.j) == mappings(y.j) && x.direction == y.direction && x.edgeLabel < y.edgeLabel) ||
              (mappings(x.j) == mappings(y.j) && x.direction == y.direction && x.edgeLabel == y.edgeLabel && x.jLabel < y.jLabel))
        //        println("candidate edges " + edgesToAssignedNodes)
        //        println("edges sorted for forward " + edgesToAssignedNodes)
        val forwardToAssigned = edgesToAssignedNodes.remove(0)
        //        println("forward to assigned " + forwardToAssigned)
        edgesToAssignedNodes = edgesToAssignedNodes.sortWith(
          //sort order of backward edges
          (x, y) =>
            mappings(x.j) < mappings(y.j) ||
              (mappings(x.j) == mappings(y.j) && x.edgeLabel < y.edgeLabel) ||
              (mappings(x.j) == mappings(y.j) && x.edgeLabel == y.edgeLabel && x.direction > y.direction))
        //        println("back from assigned " + edgesToAssignedNodes)

        {
          val e = Edge.makeEdge(mappings(forwardToAssigned.j), nextId, forwardToAssigned.jLabel, forwardToAssigned.edgeLabel, forwardToAssigned.iLabel, Edge.oppositeDirection(forwardToAssigned.direction))
          val comp = this.code(nbEdgesDone).compare(e)
          //          println("forward " + e + " " + comp + " against " + nbEdgesDone)
          if (comp > 0) {
            return true
          } else if (comp < 0) {
            badChoice = true
          }
        }
        if (!badChoice) {
          //backward edges from the new assigned node
          var i = 0
          for (e <- edgesToAssignedNodes if !badChoice) {
            i = i + 1
            val eR = Edge.makeEdge(nextId, mappings(e.j), e.iLabel, e.edgeLabel, e.jLabel, e.direction)
            val comp = this.code(nbEdgesDone + i).compare(eR)
            //            println("backward " + eR + " " + comp + " against " + (nbEdgesDone + i))
            if (comp > 0) {
              return true
            } else if (comp < 0) {
              badChoice = true
            }
          }
          if (!badChoice) {
            //recursion
            val reachable = futureExtensions.map(e => e.oppositeEnd(assigned)).toSet
            mappings(assigned) = nextId
            val rec = recNonCanonical(reachable :: cleanedAssignable, mappings, graph, nextId + 1, nbEdgesDone + edgesToAssignedNodes.size + 1)
            if (rec) {
              return true
            }
            mappings(assigned) = -1
          }
        }
      }
      return false
    }
  }

  //  //return rules DFSCode -> Edge such that the rule is safes, i.e. DFSCode is connected and vertices in Edge appear in DFSCode
  //  def getValidRules(): List[(DFSCode, Edge)] = {
  //    val graph: Seq[GraphNode] = genGraph()
  //    var validVertices: Set[Int] = Set()
  //    for (i <- 0 until graph.length) {
  //      if (graph(i).degree > 1) {
  //        validVertices = validVertices + i
  //      }
  //    }
  //    var r: List[(DFSCode, Edge)] = Nil
  //    for (e: Edge <- this.code) {
  //      //if edge can be removed without removing nodes
  //      if (validVertices.contains(e.i) && validVertices.contains(e.j)) {
  //        //check if graph would be connected without it
  //        val vToExplore: Stack[Int] = Stack(0)
  //        var vSeen: Set[Int] = Set(0)
  //        while (!vToExplore.isEmpty) {
  //          val v = vToExplore.pop()
  //          for (incidentEdge: Edge <- graph(v).incidentEdges) {
  //            val otherV = incidentEdge.oppositeEnd(v)
  //            if (!vSeen.contains(otherV)) {
  //              vToExplore.push(otherV)
  //              vSeen = vSeen + otherV
  //            }
  //          }
  //        }
  //        if (vSeen.size == this.vertexCount) {
  //          r = this.genCode(graph, e) :: r
  //        }
  //      }
  //    }
  //    return r
  //  }

  //  //gen minimal code with this edge removed
  //  private def genCode(graph: Seq[GraphNode], removedEdge: Edge): (DFSCode, Edge) = {
  //    //find min edges, which are good starting points to build the minimal code
  //    var minLabel: Int = Int.MaxValue
  //    var startingNodes: Set[Int] = Set.empty
  //    for (edge: Edge <- this.code) {
  //      if (!edge.equals(removedEdge)) {
  //        if (edge.label == minLabel) {
  //          var root: Int = edge.i
  //          if (!edge.fromItoJ) root = edge.i
  //          startingNodes = startingNodes + root
  //        } else if (edge.label < minLabel) {
  //          minLabel = edge.label
  //          var root: Int = edge.i
  //          if (!edge.fromItoJ) root = edge.i
  //          startingNodes = Set(root)
  //        }
  //      }
  //    }
  //    var currentKnownBestCode: List[Edge] = null
  //    val startingCode: List[Edge] = List(new Edge(0, 1, minLabel, true))
  //    for (n: GraphNode <- graph) {
  //      n.dfsId = -1
  //    }
  //    for (startingNode: Int <- startingNodes) {
  //      graph(startingNode).dfsId = 0
  //      currentKnownBestCode = genMinimalCode(graph, removedEdge, startingCode, currentKnownBestCode, startingNode, null)
  //    }
  //    return (new DFSCode(currentKnownBestCode, this.vertexCount), removedEdge)
  //  }

  //  private def genMinimalCode(graph: Seq[GraphNode], removedEdge: Edge, currentCode: List[Edge], currentKnownBest: List[Edge], lastAssignedNodeId: Int, parentEdge: Edge): List[Edge] = {
  //    //split incident edges in 2: backwards and forwards according to the current DFS
  //    val (bEdges, fEdges) = graph(lastAssignedNodeId).incidentEdges.foldLeft((List[Edge](), List[Edge]()))((acc: (List[Edge], List[Edge]), e: Edge) => if (e.equals(removedEdge)) { (acc._1, acc._2) } else if (e.isForwardEdge(graph, lastAssignedNodeId)) { (acc._1, e :: acc._2) } else if (e.equals(parentEdge)) { (acc._1, acc._2) } else { (e :: acc._1, acc._2) })
  //    //we add backward edges first
  //    //TODO take label and direction into account for parallel edges
  //    val bEdgesOrdered = bEdges.sortWith((x, y) => graph(x.oppositeEnd(lastAssignedNodeId)).dfsId < graph(y.oppositeEnd(lastAssignedNodeId)).dfsId || (graph(x.oppositeEnd(lastAssignedNodeId)).dfsId == graph(y.oppositeEnd(lastAssignedNodeId)).dfsId && x.label < y.label) || (graph(x.oppositeEnd(lastAssignedNodeId)).dfsId == graph(y.oppositeEnd(lastAssignedNodeId)).dfsId && x.label == y.label && x.fromItoJ && !y.fromItoJ))
  //    for (e: Edge <- bEdgesOrdered) {
  //      val comp = matcher.backwardEdge(graph(vertexId), graph(e.oppositeEnd(vertexId)), e)
  //      // if we find a difference, we exit here
  //      if (comp != 0) return (comp, -1, -1)
  //    }
  //    return null
  //  }

  override def toString: String =
    "DFSCode (" + this.code.mkString(",") + ")"

}

object DFSCode {
  val CFLIndexGen = new AtomicInteger(0)
  def main(args: Array[String]) {
    //FrequentGraph (DFSCode (Edge (0, 1, 0, 17, 0, 1)  F ,Edge (1, 0, 0, 34, 0, -1)  B ,Edge (1, 0, 0, 18, 0, -1)  B ), freq 172)
    //FrequentGraph (DFSCode (Edge (0, 1, 0, 17, 0, 1)  F ,Edge (1, 0, 0, 18, 0, -1)  B ,Edge (1, 0, 0, 34, 0, -1)  B ), freq 172) 
    //     (Edge (0, 1, 0, 947644, 0, 1)  F ,Edge (1, 2, 0, 947816, 0, 1)  F ,Edge (2, 1, 0, 947653, 0, 1)  B ,Edge (1, 3, 0, 947644, 0, -1)  F ) (4)  with Edge (3, 2, 0, 947816, 0, 1) 
    var c = new DFSCode(List(Edge.makeEdge(0, 1, 0, 947644, 0, 1), Edge.makeEdge(1, 2, 0, 947816, 0, 1), Edge.makeEdge(2, 1, 0, 947653, 0, 1), Edge.makeEdge(1, 3, 0, 947644, 0, -1), Edge.makeEdge(3, 2, 0, 947816, 0, 1)), 4)
    //        var c = new DFSCode(List(Edge.makeEdge(0, 1, 0, 947644, 0, 1), Edge.makeEdge(1, 2, 0, 947644, 0, 1), Edge.makeEdge(2, 1, 0, 947644, 0, 1), Edge.makeEdge(1, 3, 0, 947644, 0, -1), Edge.makeEdge(3, 2, 0, 947644, 0, 1)), 4)
    //TODO SHOULD BE FALSE!!!!
    println(c)
    println(c.isCanonical)
    println("###")
    //    c = new DFSCode(List(Edge.makeEdge(0, 1, 0, 947644, 0, 1), Edge.makeEdge(1, 2, 0, 947816, 0, 1), Edge.makeEdge(2, 1, 0, 947653, 0, 1), Edge.makeEdge(2, 3, 0, 947816, 0, -1), Edge.makeEdge(3, 1, 0, 947644, 0, 1)), 4)
    //    println(c)
    //    println(c.isCanonical)
  }
}
