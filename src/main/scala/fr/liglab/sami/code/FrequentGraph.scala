package fr.liglab.sami.code

case class FrequentGraph(val graph: DFSCode, val freq: Int/*, val nbEmbeddings: Long*/) extends Serializable {

//  def this(graph: DFSCode, freq: Int) = {
//    this(graph, freq, 0L)
//  }
  override def toString: String =
    "FrequentGraph (" + graph + ", freq " + freq /*+ ", embeddings " + nbEmbeddings */+ ")"
}