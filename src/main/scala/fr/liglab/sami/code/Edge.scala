package fr.liglab.sami.code
class Edge private (val i: Int, val j: Int, val iLabel: Int, val edgeLabel: Int, val jLabel: Int, val direction: Byte) extends Serializable with Equals with Ordered[Edge] {
  def this(e: Edge) = this(e.i, e.j, e.iLabel, e.edgeLabel, e.jLabel, e.direction)

  def isForwardEdge(): Boolean = {
    i < j
  }
  def oppositeEnd(vertexId: Int): Int = {
    if (this.i == vertexId) this.j else this.i
  }
  def isForwardEdge(graph: Seq[GraphNode], vertexId: Int): Boolean = {
    if (i == vertexId) {
      return graph(j).dfsId == -1
    } else {
      return graph(i).dfsId == -1
    }
  }

  def unshiftFF: Edge = new Edge(this.i, this.j - 1, this.iLabel, this.edgeLabel, this.jLabel, this.direction)
  def unshiftFB: Edge = new Edge(this.j, this.i, this.jLabel, this.edgeLabel, this.iLabel, Edge.oppositeDirection(direction))

  def shiftFF: Edge = new Edge(this.i, this.j + 1, this.iLabel, this.edgeLabel, this.jLabel, this.direction)
  def shiftFB: Edge = new Edge(this.j, this.i, this.jLabel, this.edgeLabel, this.iLabel, Edge.oppositeDirection(direction))
  def shiftByOne: Edge = new Edge(this.i + 1, this.j + 1, this.iLabel, this.edgeLabel, this.jLabel, this.direction)
  def shiftBy(a: Int): Edge = new Edge(this.i + a, this.j + a, this.iLabel, this.edgeLabel, this.jLabel, this.direction)

  def asCodeStart: Edge = {
    if (this.direction > 0) {
      new Edge(0, 1, iLabel, edgeLabel, jLabel, direction)
    } else if (this.direction < 0) {
      new Edge(0, 1, jLabel, edgeLabel, iLabel, Edge.oppositeDirection(direction))
    } else if (jLabel < iLabel) {
      new Edge(0, 1, jLabel, edgeLabel, iLabel, direction)
    } else {
      new Edge(0, 1, iLabel, edgeLabel, jLabel, direction)
    }
  }

  override def equals(that: Any): Boolean =
    that match {
      case that: Edge => that.canEqual(this) && this.i == that.i && this.j == that.j && this.iLabel == that.iLabel && this.edgeLabel == that.edgeLabel && this.jLabel == that.jLabel && this.direction == that.direction
      case _          => false
    }

  def canEqual(other: Any) = other.isInstanceOf[Edge]

  override def hashCode: Int = {
    val prime = 31
    var result = 1
    result = prime * result + i;
    result = prime * result + j;
    result = prime * result + iLabel;
    result = prime * result + edgeLabel;
    result = prime * result + jLabel;
    result = prime * result + direction;
    result
  }

  //ref definition 5 of gSpan paper
  override def compare(that: Edge): Int = {
    //println("comp " + this + " with " + that)
    if (this.isForwardEdge()) {
      if (!that.isForwardEdge()) return 1
      //both forward
      if (that.i < this.i) return -1
      if (that.i > this.i) return 1

      if (this.direction > that.direction) return -1
      if (this.direction < that.direction) return 1

      if (this.iLabel < that.iLabel) return -1
      if (this.iLabel > that.iLabel) return 1

      if (this.edgeLabel < that.edgeLabel) return -1
      if (this.edgeLabel > that.edgeLabel) return 1

      if (this.jLabel < that.jLabel) return -1
      if (this.jLabel > that.jLabel) return 1

    } else {
      if (that.isForwardEdge()) return -1
      //both backward
      if (this.j < that.j) return -1
      if (this.j > that.j) return 1

      if (this.direction > that.direction) return -1
      if (this.direction < that.direction) return 1

      if (this.edgeLabel < that.edgeLabel) return -1
      if (this.edgeLabel > that.edgeLabel) return 1

    }
    return 0
  }
  override def toString: String = {
    var res = "Edge (" + i + ", " + j + ", " + iLabel + ", " + edgeLabel + ", " + jLabel + ", " + direction + ") "
    if (this.isForwardEdge()) { return res + " F " }
    return res + " B "
  }
}

object Edge {
  def DIR_ANY: Byte = 0
  def DIR_OUT: Byte = 1
  def DIR_IN: Byte = -1

  def makeUndirEdge(i: Int, j: Int, iLabel: Int, edgeLabel: Int, jLabel: Int): Edge = {
    new Edge(i, j, iLabel, edgeLabel, jLabel, DIR_ANY)
  }
  def makeOutEdge(i: Int, j: Int, iLabel: Int, edgeLabel: Int, jLabel: Int): Edge = {
    new Edge(i, j, iLabel, edgeLabel, jLabel, DIR_OUT)
  }
  def makeInEdge(i: Int, j: Int, iLabel: Int, edgeLabel: Int, jLabel: Int): Edge = {
    new Edge(i, j, iLabel, edgeLabel, jLabel, DIR_IN)
  }

  def makeEdge(i: Int, j: Int, iLabel: Int, edgeLabel: Int, jLabel: Int, direction: Byte): Edge = {
    new Edge(i, j, iLabel, edgeLabel, jLabel, direction)
  }

  def oppositeDirection(direction: Byte): Byte = (-direction).byteValue()
  def oppositeDirection(e: Edge): Byte = oppositeDirection(e.direction)
  def isOriented(e: Edge): Boolean = isOriented(e.direction)
  def isOriented(d: Byte): Boolean = d != DIR_ANY
  def isNotIn(e: Edge): Boolean = isNotIn(e.direction)
  def isNotIn(d: Byte): Boolean = d != DIR_IN

  def equalAndOppositeDir(e1: Edge, e2: Edge): Boolean = {
    return e1.direction == e2.direction && e1.j == e2.i && e1.i == e2.j && e1.iLabel == e2.jLabel && e1.edgeLabel == e2.edgeLabel && e1.jLabel == e2.iLabel
  }

  def notEqualAndAnyDir(e1: Edge, e2: Edge): Boolean = {
    return (e1.direction == e2.direction && (e1.i != e2.i || e1.j != e2.j || e1.iLabel != e2.iLabel || e1.edgeLabel != e2.edgeLabel || e1.jLabel != e2.jLabel)) ||
      (e1.direction != e2.direction && (e1.i != e2.j || e1.j != e2.i || e1.iLabel != e2.jLabel || e1.edgeLabel != e2.edgeLabel || e1.jLabel != e2.iLabel))
  }
}
