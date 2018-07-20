package fr.liglab.sami.code

import fr.liglab.sami.code.IntersectType.IntersectType
import fr.liglab.sami.data.GraphInfo
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.locks.Lock
import java.io.IOException
import java.io.ObjectInputStream
import fr.liglab.sami.utilities.Marshal

abstract class DagProvider extends Serializable {
  def get: DAG
  def isCanonical: Boolean
  def tryLock: Boolean
}

class ComputedDag(private val dag: DAG, private val canonical: Boolean = true) extends DagProvider with Serializable {
  override def get: DAG = {
    dag
  }
  override def isCanonical = canonical
  override def tryLock = true
}

class JoinDag(@volatile private var parentDag: DAG, private var extensionEdge: Edge, val minSup: Int, automorphTransfers: Array[List[Int]]) extends DagProvider with Serializable {
  // println("constructing JoinDag")
  @volatile private var dag: DAG = null
  @transient private var lock: Lock = new ReentrantLock()

  override def tryLock: Boolean = {
    if (parentDag == null) return true
    if (lock.tryLock()) {
      if (parentDag == null) {
        lock.unlock()
        return true
      } else {
        if (parentDag.tryLock) {
          return true
        } else {
          lock.unlock()
          return false
        }
      }
    } else {
      return false
    }
  }
  @throws(classOf[IOException])
  private def readObject(in: ObjectInputStream): Unit = {
    in.defaultReadObject()
    this.lock = new ReentrantLock()
  }

  override def get: DAG = {
    if (parentDag != null) {
      if (!lock.tryLock()) {
        throw new RuntimeException("only supposed to go there when we actually have the lock!")
      }
      if (parentDag != null) {
        //        println(Thread.currentThread.getName() + " performing JoinDag")
        try {
          this.dag = parentDag.extend(DagProvider.graphData.getExtensions(extensionEdge), minSup, false, automorphTransfers)
        } catch {
          case e: Exception => {
            //          e.printStackTrace()
            println(Thread.currentThread().getName + " exception in lazy extend")
            Marshal.dumpToFile("exception_lazy.dag1", parentDag)
            Marshal.dumpToFile("exception_lazy.dag2", DagProvider.graphData.getExtensions(extensionEdge))
            throw e
          }
        }
        if (this.dag.MNISupport < minSup) {
          this.dag = null
        }
        this.parentDag = null
        this.extensionEdge = null
        // println(Thread.currentThread.getName() + " completed JoinDag")
      }
      lock.unlock()
    }
    return dag
  }
  override def isCanonical = false
}

class IntersectDag(@volatile private var d1: DagProvider, private var d2: DagProvider, private val t: IntersectType, val minSup: Int, useProjection:Boolean, automorphTransfers: Array[List[Int]], val doubleFFpos: Int) extends DagProvider with Serializable {
  // println("constructing IntersectDag")
  @volatile private var dag: DAG = null
  @transient private var lock: Lock = new ReentrantLock()

  //only 1 of d1 or d2 actually has a lock
  override def tryLock: Boolean = {
    if (d1 == null) return true
    if (lock.tryLock()) {
      if (d1 == null) {
        lock.unlock()
        return true
      } else {
        if (d1.tryLock && d2.tryLock) {
          return true
        } else {
          lock.unlock()
          return false
        }
      }
    } else {
      return false
    }
  }

  @throws(classOf[IOException])
  private def readObject(in: ObjectInputStream): Unit = {
    in.defaultReadObject()
    this.lock = new ReentrantLock()
  }

  override def get: DAG = {
    if (d1 != null) {
      if (!lock.tryLock()) {
        throw new RuntimeException("only supposed to go there when we actually have the lock!")
      }
      if (d1 != null) {
        // println(Thread.currentThread.getName() + " performing IntersectDag")
        val d1g = d1.get
        val d2g = d2.get
        if (d1g != null && d2g != null) {
          this.dag = try {
            t match {
              case IntersectType.FFtoFF => d1g.intersectFFtoFF(d2g, minSup, false, automorphTransfers, doubleFFpos)
              case IntersectType.FFtoFB => d1g.intersectFFtoFB(d2g, minSup, false, automorphTransfers, useProjection)
              case IntersectType.BBtoBB => d1g.intersectBBtoBB(d2g, minSup, false, automorphTransfers)
              case IntersectType.BFtoBF => d1g.intersectBFtoBF(d2g, minSup, false, automorphTransfers)
              case IntersectType.extend => d1g.extend(d2g, minSup, false, automorphTransfers)
            }
          } catch {
            case e: Exception => {
              //          e.printStackTrace()
              println(Thread.currentThread().getName + " exception " + t + "-" + doubleFFpos + " on lazy")
              Marshal.dumpToFile("exception_" + t + "_lazy.dag1", d1g)
              Marshal.dumpToFile("exception_" + t + "_lazy.dag2", d2g)
              throw e
            }
          }
          if (this.dag.MNISupport < minSup) {
            this.dag = null
          }
        }
        this.d1 = null
        this.d2 = null
        // println(Thread.currentThread.getName() + " completed IntersectDag")
      }
      lock.unlock()
    }
    return dag
  }
  override def isCanonical = false
}

class SeedDag(val edge: Edge) extends DagProvider {
  override def isCanonical = true
  override def tryLock = true
  override def get: DAG = {
    DagProvider.graphData.getExtensions(edge)
  }
}

object DagProvider {
  var graphData: GraphInfo = null
}
