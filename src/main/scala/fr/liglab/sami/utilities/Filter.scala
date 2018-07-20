package fr.liglab.sami.utilities

import java.io.IOException
import java.io.ObjectOutputStream

import com.google.common.hash.BloomFilter

import net.openhft.koloboke.collect.set.IntSet
import java.io.ObjectInputStream
import net.openhft.koloboke.collect.set.hash.HashIntSets

trait Filter {
  def compatible(elem: Int): Boolean
  def isExact: Boolean
  def merge(f: Filter): Boolean
  def doesFilter: Boolean
}

case class ExactFilter(var f: IntSet) extends Filter with Serializable {
  override def toString(): String = "Exact_" + f.size
  override def compatible(elem: Int) = f.contains(elem)
  override def isExact = true
  override def merge(f: Filter) = false
  override def doesFilter = true
  @throws(classOf[IOException])
  private def writeObject(out: ObjectOutputStream): Unit = {
    out.writeInt(f.size)
    val c = f.cursor()
    while (c.moveNext()) {
      out.writeInt(c.elem())
    }
  }

  @throws(classOf[IOException])
  private def readObject(in: ObjectInputStream): Unit = {
    val size = in.readInt()
    this.f = HashIntSets.newMutableSet(size)
    for (i <- 0 until size) {
      f.add(in.readInt())
    }
  }
}

case class ProbabilisticFilter(var f: BloomFilter[Integer]) extends Filter with Serializable {
  override def compatible(elem: Int) = f == null || f.mightContain(elem)
  override def isExact = false
  override def toString(): String = "Bloom_" + (if (f == null) "rem" else f.approximateElementCount())
  override def doesFilter = f != null
  override def merge(f: Filter): Boolean = {
    if (this.f == null) return true
    f match {
      case ExactFilter(f1) => {
        val c = f1.cursor()
        while (c.moveNext()) {
          this.f.put(c.elem())
        }
      }
      case ProbabilisticFilter(f1) => {
        if (f1 == null) this.f = null else this.f.putAll(f1)
      }
    }
    if (this.f != null && this.f.expectedFpp > .5) this.f = null
    return true
  }
  @throws(classOf[IOException])
  private def writeObject(out: ObjectOutputStream): Unit = {
    out.writeObject(f)
  }

  @throws(classOf[IOException])
  private def readObject(in: ObjectInputStream): Unit = {
    this.f = in.readObject().asInstanceOf[BloomFilter[Integer]]
  }
}