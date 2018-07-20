package fr.liglab.sami.code

class GraphNode(idInCode: Int) {
  var incidentEdges: List[Edge] = List()
  var dfsId = -1

  def getIdInCode(): Int = {
    idInCode
  }
  def addEdge(e: Edge) = {
    this.incidentEdges = e :: this.incidentEdges
  }
  def degree: Int = {
    this.incidentEdges.size
  }
  override def toString: String =
    "Node " + idInCode
}