package fr.liglab.sami.code
import scala.math.Ordered.orderingToOrdered
import java.io.IOException
import java.io.ObjectInputStream
import java.io.ObjectOutputStream
import java.io.IOException

class IncidentEdge(var otherV: Int, var iLabel: Int, var edgeLabel: Int, var jLabel: Int, var direction: Byte) extends Serializable with Equals with Ordered[IncidentEdge] {

  override def toString: String =
    "IncidentEdge (" + otherV + ", " + iLabel + ", " + edgeLabel + ", " + jLabel + ", " + direction + ")"

  override def equals(that: Any) =
    that match {
      case that: IncidentEdge => that.canEqual(this) && that.otherV.equals(this.otherV) && that.iLabel.equals(this.iLabel) && that.edgeLabel.equals(this.edgeLabel) && that.jLabel.equals(this.jLabel) && that.direction.equals(this.direction)
      case _             => false
    }

  def canEqual(other: Any) = other.isInstanceOf[IncidentEdge]

  def toTuple(): Tuple5[Int,Int,Int,Int,Byte] = {
    return Tuple5(otherV,iLabel,edgeLabel,jLabel,direction)
  }

  def compare (other: IncidentEdge) = this.toTuple compare other.toTuple


  @throws(classOf[IOException])
  private def writeObject(out: ObjectOutputStream): Unit = {
    out.writeInt(otherV)
    out.writeInt(iLabel)
    out.writeInt(edgeLabel)
    out.writeInt(jLabel)
    out.write(direction)
  }

    @throws(classOf[IOException])
    private def readObject(in: ObjectInputStream): Unit = {
      otherV = in.readInt()
      iLabel = in.readInt()
      edgeLabel = in.readInt()
      jLabel = in.readInt()
      direction = in.readByte()
    }
}
