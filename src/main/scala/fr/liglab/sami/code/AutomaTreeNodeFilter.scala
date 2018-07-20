package fr.liglab.sami.code

import java.io.IOException
import java.io.ObjectInputStream
import java.io.ObjectOutputStream
import java.util.TreeSet
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.ListBuffer

import org.eclipse.collections.impl.list.mutable.primitive.IntArrayList
import org.eclipse.collections.impl.map.mutable.UnifiedMap
import org.eclipse.collections.impl.map.mutable.primitive.IntIntHashMap

import fr.liglab.sami.code.IntersectType.IntersectType
import fr.liglab.sami.utilities.BFInstantiator
import fr.liglab.sami.utilities.ExactFilter
import fr.liglab.sami.utilities.FakeMap
import fr.liglab.sami.utilities.ProbabilisticFilter
import net.openhft.koloboke.collect.map.IntIntMap
import net.openhft.koloboke.collect.map.IntObjMap
import net.openhft.koloboke.collect.map.LongObjMap
import net.openhft.koloboke.collect.map.ObjObjMap
import net.openhft.koloboke.collect.map.hash.HashIntIntMapFactory
import net.openhft.koloboke.collect.map.hash.HashIntIntMaps
import net.openhft.koloboke.collect.map.hash.HashIntObjMap
import net.openhft.koloboke.collect.map.hash.HashIntObjMaps
import net.openhft.koloboke.collect.map.hash.HashLongObjMaps
import net.openhft.koloboke.collect.map.hash.HashObjObjMap
import net.openhft.koloboke.collect.map.hash.HashObjObjMaps
import net.openhft.koloboke.collect.set.IntSet
import net.openhft.koloboke.collect.set.hash.HashIntSets

//implementation based on automata principles
@SerialVersionUID(1L)
class AutomaTreeNodeFilter(var nbNodes: Int, var parallelValidation: Boolean) extends DAG with Serializable {
  //  private var debug = false
  //  private var preDebug = false
  private var nodeIdDistributor = new AtomicInteger(1)
  private var root: AutomatonNode = null
  private var nodesByDepth: Array[IndexedSeq[AutomatonNode]] = null
  private var MNISupportCached: Int = -1
  private var transByLevel: Array[IntSet] = null

  def this(instances: Iterable[(Int, Int)], parallelValidation: Boolean) {
    this(2, parallelValidation)
    val vToSuccessors: IntObjMap[IntSet] = AutomaTreeNodeFilter.instantiateIntObjectMap[IntSet]
    instances.foreach {
      p =>
        var suc = vToSuccessors.get(p._1)
        if (suc == null) {
          suc = AutomaTreeNodeFilter.instantiateIntSet
          vToSuccessors.put(p._1, suc)
        }
        suc.add(p._2)
    }
    val transitions = AutomaTreeNodeFilter.groupExtendedTransitions(this)(vToSuccessors)
    this.root = this.newNode(0, 0, transitions)
  }

  def this(vToSuccessors: IntObjMap[IntSet], parallelValidation: Boolean) {
    this(2, parallelValidation)
    val transitions = AutomaTreeNodeFilter.groupExtendedTransitions(this)(vToSuccessors)
    this.root = this.newNode(0, 0, transitions)
  }

  @throws(classOf[IOException])
  private def writeObject(out: ObjectOutputStream): Unit = {
    out.writeObject(root)
    out.writeInt(nbNodes)
    out.writeBoolean(parallelValidation)
    out.writeInt(MNISupportCached)
  }

  @throws(classOf[IOException])
  private def readObject(in: ObjectInputStream): Unit = {
    root = in.readObject().asInstanceOf[AutomatonNode]
    nbNodes = in.readInt()
    parallelValidation = in.readBoolean()
    MNISupportCached = in.readInt()
  }

  def MNISupport: Int = getMNISupport()
  def leafSet: IntSet = getLeafSet()
  def paths: ListBuffer[Seq[Int]] = getPaths()
  def isEmpty: Boolean = this.root == null || this.root.transitions.isEmpty()

  def hasEdgeFrom(k: Int): Boolean = this.root.transitions.containsKey(k)

  def getDestinationsFor(k: Int): IntSet = if (this.root.transitions.containsKey(k)) this.root.transitions.get(k)(0).transitions.keySet() else null

  private def getNextNodeId: Int = {
    val id = this.nodeIdDistributor.getAndIncrement
    if (id < 0) throw new RuntimeException("ran out of IDs")
    //    println("giving id " + id + " next " + this.nodeIdDistributor)
    return id
  }
  private def getMNISupport(): Int = {
    if (this.root == null) {
      this.MNISupportCached = 0
    } else if (this.MNISupportCached == -1) {
      val distinctValsByDepth: Array[IntSet] = Array.fill(this.nbNodes)(AutomaTreeNodeFilter.instantiateIntSet)
      def updateMniFunction(n: AutomatonNode) = {
        distinctValsByDepth(n.codeVertexId).addAll(n.transitions.keySet())
      }
      this.applyToEachDagNode(updateMniFunction)
      var min = Int.MaxValue
      for (s: IntSet <- distinctValsByDepth) {
        min = Math.min(min, s.size)
      }
      this.MNISupportCached = min
    }
    return this.MNISupportCached
  }

  def getNbStatesAndTransitions(): (Long, Long) = {
    var nbStates = 0L
    var nbTrans = 0L
    def updateCounts(n: AutomatonNode) = {
      nbStates = nbStates + 1
      nbTrans = nbTrans + n.transitions.size()
    }
    if (root == null) return (0L, 0L)
    this.applyToEachDagNode(updateCounts)
    return (nbStates, nbTrans)
  }

  def getNodesByLevel(): List[mutable.Set[AutomatonNode]] = {
    val distinctNodesByDepth: List[mutable.Set[AutomatonNode]] = List.fill(this.nbNodes)(mutable.Set.empty)
    def collectNodesFunction(n: AutomatonNode) = {
      distinctNodesByDepth(n.codeVertexId) += n
    }
    this.applyToEachDagNode(collectNodesFunction)
    return distinctNodesByDepth
  }

  def genNodesByDepth = {
    val byLevel = getNodesByLevel
    this.nodesByDepth = Array.fill(this.nbNodes)(mutable.ArrayBuffer.empty)
    for (i <- 0 until this.nbNodes) {
      this.nodesByDepth(i).++=(byLevel(i))
    }
  }

  def getTransByLevel(): Array[IntSet] = {
    if (this.transByLevel == null) {
      this.synchronized {
        this.transByLevel = Array.fill(this.nbNodes)(AutomaTreeNodeFilter.instantiateIntSet)
        this.applyToEachDagNode(n => transByLevel(n.codeVertexId).addAll(n.transitions.keySet()))
      }
    }
    return this.transByLevel
  }

  private def getLeafSet(): IntSet = {
    var leafSet = AutomaTreeNodeFilter.instantiateIntSet
    def updateLeafSetFunction(n: AutomaTreeNodeFilter#AutomatonNode) = {
      if (n.codeVertexId == this.nbNodes - 1) {
        leafSet.addAll(n.transitions.keySet())
      }
    }
    this.applyToEachDagNode(updateLeafSetFunction)
    return leafSet
  }
  //
  private def getPaths(): ListBuffer[Seq[Int]] = {
    val pathBuf = new ListBuffer[Seq[Int]]()
    val p = ArrayBuffer.fill(this.nbNodes)(-1)
    def recPaths(stack: List[AutomaTreeNodeFilter#AutomatonNode]): Unit = {
      if (stack.isEmpty) {
        pathBuf += p.toList
      } else {
        val n = stack.head
        val c = n.transitions.cursor()
        while (c.moveNext()) {
          //careful! linear cost in path length, a set may be more appropriate
          if (!p.contains(c.key())) {
            p(n.codeVertexId) = c.key()
            if (c.value() != null) {
              recPaths(c.value().toList ::: stack.tail)
            } else {
              recPaths(stack.tail)
            }
          }
        }
        p(n.codeVertexId) = -1
      }
    }
    if (this.root != null) recPaths(List(this.root))
    return pathBuf
  }

  def nbEmbeddings(): Long = {
    val p = AutomaTreeNodeFilter.instantiateIntSet
    var count = 0L
    def recPaths(stack: List[AutomaTreeNodeFilter#AutomatonNode]): Unit = {
      if (stack.isEmpty) {
        count += 1
      } else {
        val n = stack.head
        val c = n.transitions.cursor()
        while (c.moveNext()) {
          if (p.add(c.key())) {
            if (p.size() != n.codeVertexId + 1) {
              throw new RuntimeException("hoho")
            }
            if (c.value() != null) {
              recPaths(c.value().toList ::: stack.tail)
            } else {
              recPaths(stack.tail)
            }
            p.remove(c.key())
          }
        }
      }
    }
    if (this.root != null) recPaths(List(this.root))
    return count
  }

  def getNbEmbeddingStorageUnits(): List[Int] = List[Int]()

  //
  //  def hasEmbedding(embedding: Seq[Int]): Boolean = {
  //    var node = this.root
  //    for (t <- embedding) {
  //      node = node.transitions.get(t)
  //      if (node == null) {
  //        return false
  //      }
  //    }
  //    return true
  //  }
  //
  def describe: Unit = {
    if (this.root == null) {
      println("empty tree")
      return
    }
    val nodesDist = Array.fill(this.nbNodes)(0)
    val transDist = Array.fill(this.nbNodes)(0)
    this.applyToEachDagNode(n => {
      nodesDist(n.codeVertexId) += 1
      transDist(n.codeVertexId) += n.transitions.size()
    })
    for (i <- 0 until nodesDist.size) println(i + " " + nodesDist(i) + " " + transDist(i))
  }

  def hasEmbedding(embed: List[Int]): List[Int] = {
    val positions = new Array[Int](embed.size)
    def recHasEmbedding(toMatch: List[Int], toDo: List[AutomatonNode]): Boolean = {
      if (toMatch.isEmpty) return true
      val n = toDo.head
      if (!n.transitions.containsKey(toMatch.head)) return false
      positions(embed.size - toMatch.size) = n.id
      val trans = n.transitions.get(toMatch.head)
      if (trans == null) {
        return recHasEmbedding(toMatch.tail, toDo.tail)
      } else {
        return recHasEmbedding(toMatch.tail, trans.toList ::: toDo.tail)
      }
    }
    if (recHasEmbedding(embed, List(this.root))) {
      return positions.toList
    } else {
      return null
    }
  }

  def mniSetsFromEmbeddings(mniSets: Array[IntSet]): Unit = {
    val p = ArrayBuffer.fill(this.nbNodes)(-1)
    def recPaths(stack: List[AutomaTreeNodeFilter#AutomatonNode]): Unit = {
      if (stack.isEmpty) {
        for (i <- 0 until p.size) {
          mniSets(i).add(p(i))
        }
      } else {
        val n = stack.head
        val c = n.transitions.cursor()
        while (c.moveNext()) {
          //careful! linear cost in path length, a set may be more appropriate
          if (!p.contains(c.key())) {
            p(n.codeVertexId) = c.key()
            if (c.value() != null) {
              recPaths(c.value().toList ::: stack.tail)
            } else {
              recPaths(stack.tail)
            }
            p(n.codeVertexId) = -1
          }
        }
      }
    }
    if (this.root != null) recPaths(List(this.root))
  }

  def applyToEachDagNode(f: AutomatonNode => Unit) = {
    val visited: IntSet = AutomaTreeNodeFilter.instantiateIntSet
    if (root == null) println("root is null")
    if (root != null) recApplyToEachDagNode(this.root, f)
    def recApplyToEachDagNode(n: AutomatonNode, f: AutomatonNode => Unit): Unit = {
      f(n)
      if (n.transitions == null) throw new RuntimeException("null trans at " + n)
      val c = n.transitions.cursor()
      while (c.moveNext()) {
        val suc = c.value()
        if (suc != null) {
          for (s <- suc) {
            if (visited.add(s.id)) {
              recApplyToEachDagNode(s, f)
            }
          }
        }
      }
    }
  }

  override def extend(other: DAG, threshold: Int, lazyCleanup: Boolean, automorph: Array[List[Int]]): DAG = {
    val resAutomaton = new AutomaTreeNodeFilter(this.nbNodes + 1, this.parallelValidation)
    //    try {
    val d2 = other.asInstanceOf[AutomaTreeNodeFilter]
    resAutomaton.nodesByDepth = Array.fill(resAutomaton.nbNodes)(IndexedSeq.empty)
    val extensions: IntObjMap[_ <: ArrayBuffer[_ <: AutomaTreeNodeFilter#AutomatonNode]] = d2.root.transitions
    resAutomaton.root = AutomaTreeNodeFilter.extend(resAutomaton)(this.root, extensions, threshold)
    //    val mniSets: Array[IntSet] = Array.fill(resAutomaton.nbNodes)(HashIntSets.newMutableSet())
    //    resAutomaton.mniSetsFromEmbeddings(mniSets)
    //    val refMniSupport = mniSets.map(_.size()).min
    //    for (d <- resAutomaton.nodesByDepth) {
    //      println("#nodes " + d.size)
    //    }
    //    println("starting validation")
    resAutomaton.validateTransitionsAndMinimize(true, threshold, lazyCleanup, automorph)
    //    if (refMniSupport > threshold && refMniSupport != resAutomaton.MNISupportCached) {
    //      val mniSetsN: Array[IntSet] = Array.fill(resAutomaton.nbNodes)(HashIntSets.newMutableSet())
    //      resAutomaton.mniSetsFromEmbeddings(mniSetsN)
    //      for (i <- 0 until resAutomaton.nbNodes) {
    //        mniSets(i).removeAll(mniSetsN(i))
    //        println("diff " + mniSets(i))
    //      }
    //      throw new RuntimeException("validate messed things up " + refMniSupport + " " + resAutomaton.MNISupportCached)
    //    }
    resAutomaton.nodesByDepth = null
    //    } catch {
    //      case re: RuntimeException => {
    //        Marshal.dumpToFile("bug_extend.dag1", this)
    //        Marshal.dumpToFile("bug_extend.dag2", other)
    //        throw re
    //      }
    //    }
    return resAutomaton
  }

  override def intersectBBtoBB(other: DAG, threshold: Int, lazyCleanup: Boolean, automorph: Array[List[Int]]): DAG = {
    val resAutomaton = new AutomaTreeNodeFilter(this.nbNodes, this.parallelValidation)
    //    try {
    val d2 = other.asInstanceOf[AutomaTreeNodeFilter]
    resAutomaton.nodesByDepth = Array.fill(resAutomaton.nbNodes)(IndexedSeq.empty)
    resAutomaton.root = AutomaTreeNodeFilter.intersect(resAutomaton)(this.root, d2.root, IntersectType.BBtoBB, threshold, -1)
    resAutomaton.validateTransitionsAndMinimize(false, threshold, lazyCleanup, automorph)
    resAutomaton.nodesByDepth = null
    //    } catch {
    //      case re: RuntimeException => {
    //        Marshal.dumpToFile("bug_intersectBBtoBB.dag1", this)
    //        Marshal.dumpToFile("bug_intersectBBtoBB.dag2", other)
    //        throw re
    //      }
    //    }
    return resAutomaton
  }
  override def intersectBFtoBF(other: DAG, threshold: Int, lazyCleanup: Boolean, automorph: Array[List[Int]]): DAG = {
    val resAutomaton = new AutomaTreeNodeFilter(this.nbNodes + 1, this.parallelValidation)
    //    try {
    val d2 = other.asInstanceOf[AutomaTreeNodeFilter]
    //    this.describe
    //    d2.describe
    resAutomaton.nodesByDepth = Array.fill(resAutomaton.nbNodes)(IndexedSeq.empty)
    resAutomaton.root = AutomaTreeNodeFilter.intersect(resAutomaton)(this.root, d2.root, IntersectType.BFtoBF, threshold, -1)
    //    resAutomaton.describe
    resAutomaton.validateTransitionsAndMinimize(false, threshold, lazyCleanup, automorph)
    resAutomaton.nodesByDepth = null
    //    } catch {
    //      case re: RuntimeException => {
    //        Marshal.dumpToFile("bug_intersectBFtoBF.dag1", this)
    //        Marshal.dumpToFile("bug_intersectBFtoBF.dag2", other)
    //        throw re
    //      }
    //    }
    return resAutomaton
  }
  override def intersectFFtoFF(other: DAG, threshold: Int, lazyCleanup: Boolean, automorph: Array[List[Int]], doubleFFPos: Int): DAG = {
    val resAutomaton = new AutomaTreeNodeFilter(this.nbNodes + 1, this.parallelValidation)
    //    try {
    val d2 = other.asInstanceOf[AutomaTreeNodeFilter]
    //    this.getNodesByLevel().foreach(l => println(l.size))
    //    d2.getNodesByLevel().foreach(l => println(l.size))
    resAutomaton.nodesByDepth = Array.fill(resAutomaton.nbNodes)(IndexedSeq.empty)
    //      println("building dag " + System.currentTimeMillis())
    resAutomaton.root = AutomaTreeNodeFilter.intersect(resAutomaton)(this.root, d2.root, IntersectType.FFtoFF, threshold, doubleFFPos)
    //    val mniSets: Array[IntSet] = Array.fill(resAutomaton.nbNodes)(HashIntSets.newMutableSet())
    //    resAutomaton.mniSetsFromEmbeddings(mniSets)
    //    val refMniSupport = mniSets.map(_.size()).min
    //    println("child " + resAutomaton.hasEmbedding(List(2680084, 2737777, 2059346, 2479691, 2023408, 2723979, 2306538)))
    //            println("going into validation")
    //    for (d <- resAutomaton.nodesByDepth) {
    //      println("#nodes " + d.size)
    //    }
    resAutomaton.validateTransitionsAndMinimize(false, threshold, lazyCleanup, automorph)
    //    if (refMniSupport > threshold && refMniSupport != resAutomaton.MNISupportCached) {
    //      val mniSetsN: Array[IntSet] = Array.fill(resAutomaton.nbNodes)(HashIntSets.newMutableSet())
    //      resAutomaton.mniSetsFromEmbeddings(mniSetsN)
    //      for (i <- 0 until resAutomaton.nbNodes) {
    //        mniSets(i).removeAll(mniSetsN(i))
    //        println("diff " + mniSets(i))
    //      }
    //      throw new RuntimeException("validate messed things up " + refMniSupport + " " + resAutomaton.MNISupportCached)
    //    }
    resAutomaton.nodesByDepth = null
    //    } catch {
    //      case re: RuntimeException => {
    //        Marshal.dumpToFile("bug_intersectFFtoFF.dag1", this)
    //        Marshal.dumpToFile("bug_intersectFFtoFF.dag2", other)
    //        throw re
    //      }
    //    }
    //    println("done " + System.currentTimeMillis())
    return resAutomaton
  }
  override def intersectFFtoFB(other: DAG, threshold: Int, lazyCleanup: Boolean, automorph: Array[List[Int]], useProjection: Boolean): DAG = {
    val resAutomaton = new AutomaTreeNodeFilter(this.nbNodes, this.parallelValidation)
    val d2 = other.asInstanceOf[AutomaTreeNodeFilter]
    //    println("this states and trans " + this.getNbStatesAndTransitions())
    //    println("other states and trans " + d2.getNbStatesAndTransitions())
    //    var sw = new StopWatch()
    //    sw.start()
    if (useProjection) {
      //      println("using projection")
      val d1proj = new AutomaTreeNodeFilter(this.nbNodes, this.parallelValidation)
      d1proj.root = AutomaTreeNodeFilter.project(d1proj)(this.root, d2.getTransByLevel(), threshold)
      //            d1proj.describe
      if (d1proj.root == null) return resAutomaton
      //      val d2proj = new AutomaTreeForwardRef(d2.nbNodes, d2.parallelValidation)
      //      d2proj.root = AutomaTreeForwardRef.project(d2proj)(d2.root, this.getTransByLevel(), threshold)
      //            d2proj.describe
      //      if (d2proj.root == null) return resAutomaton
      //      println("done projection")
      //            d1proj.describe
      //            d2proj.describe
      resAutomaton.nodesByDepth = Array.fill(resAutomaton.nbNodes)(IndexedSeq.empty)
      resAutomaton.root = AutomaTreeNodeFilter.intersect(resAutomaton)(d1proj.root, d2.root, IntersectType.FFtoFB, threshold, -1)
      //      resAutomaton.describe
    } else {
      resAutomaton.nodesByDepth = Array.fill(resAutomaton.nbNodes)(IndexedSeq.empty)
      resAutomaton.root = AutomaTreeNodeFilter.intersect(resAutomaton)(this.root, d2.root, IntersectType.FFtoFB, threshold, -1)
      //      resAutomaton.describe
    }
    //    println("going into validation")
    //    for (d <- resAutomaton.nodesByDepth) {
    //      println("#nodes " + d.size)
    //    }
    //    println("starting validation " + sw.getElapsedTime())
    resAutomaton.validateTransitionsAndMinimize(false, threshold, lazyCleanup, automorph)
    //    println("output states and trans " + resAutomaton.getNbStatesAndTransitions())
    resAutomaton.nodesByDepth = null
    return resAutomaton
  }

  final private def prepDag = {
    for (i <- this.nodesByDepth.size - 1 to 0 by -1) {
      if (this.parallelValidation) {
        this.nodesByDepth(i).par.foreach(n => n.prepSuccessorsAndPredecessors)
      } else {
        for (n <- this.nodesByDepth(i)) {
          n.prepSuccessorsAndPredecessors
        }
      }
    }
    //    val sw = new StopWatch
    //    sw.start()
    //    for (i <- 0 until this.nodesByDepth.size) {
    //      if (this.parallelValidation) {
    //        this.nodesByDepth(i).par.foreach(n => n.prepFilters)
    //      } else {
    //        for (n <- this.nodesByDepth(i)) {
    //          n.prepFilters
    //        }
    //      }
    //      var nbUsefulFilters = 0
    //      for (n <- this.nodesByDepth(i)) {
    //        n.conditionalTransitions.foreach(f => if (f._2.doesFilter) nbUsefulFilters += 1)
    //      }
    //      println("#usefulFilters at level " + this.nodesByDepth(i).head.codeVertexId + " = " + nbUsefulFilters)
    //    }
    //    println(sw.getElapsedTime() + " filters construction")
  }

  //  final private def globalMerge = {
  //    //stop at one because the root is alone at 0
  //    for (i <- this.nodesByDepth.size - 1 to 1 by -1) {
  //      this.mergeLevel(this.nodesByDepth(i))
  //    }
  //  }

  final private def cleanup = {
    this.nodesByDepth.foreach(_.foreach(_.deleteSuccessorsAndPredecessors()))
  }

  final private def cleanupLeaves = {
    for (n <- this.nodesByDepth(this.nodesByDepth.size - 1)) {
      if (n.predecessors.size() == 1) {
        val cursor = n.predecessors.cursor()
        cursor.moveNext()
        val trans = cursor.value()
        if (trans.size() == 1) {
          val c2 = trans.intIterator()
          val v = c2.next()
          n.transitions.remove(v)
          //          if (n.transitions.isEmpty()) {
          //            throw new RuntimeException("not supposed to be empty")
          //          }
        }
      }
    }
  }

  def validateTransitionsAndMinimize(cleanupLeaves: Boolean, threshold: Int, lazyCleanup: Boolean, automorph: Array[List[Int]]): Unit = {
    //TODO keep transitions that are not validated but are validated by a sibling to help merging more?
    if (this.root != null) {
      //      var sw = new StopWatch()
      //      sw.start()
      //      println("first minimization " + sw.getElapsedTime())
      //      this.applyToEachDagNode(n => if (n.transitions == null) throw new RuntimeException("null trans"))
      //      val rt = Runtime.getRuntime
      //      println("max memory " + rt.maxMemory() / 1000000000d + " total " + rt.totalMemory() / 1000000000d + " free " + rt.freeMemory() / 1000000000d + " used " + (rt.totalMemory() - rt.freeMemory()) / 1000000000d)
      //      val nio = sun.misc.SharedSecrets.getJavaNioAccess().getDirectBufferPool()
      //      println("native memory count " + nio.getCount + " used " + nio.getMemoryUsed / 1000000000d + " capacity " + nio.getTotalCapacity / 1000000000d)
      this.prepDag
      //      System.gc()
      //      Thread.sleep(1000)
      //      println("max memory " + rt.maxMemory() / 1000000000d + " total " + rt.totalMemory() / 1000000000d + " free " + rt.freeMemory() / 1000000000d + " used " + (rt.totalMemory() - rt.freeMemory()) / 1000000000d)
      //      println("native memory count " + nio.getCount + " used " + nio.getMemoryUsed / 1000000000d + " capacity " + nio.getTotalCapacity / 1000000000d)
      if (cleanupLeaves) {
        this.cleanupLeaves
      }
      //      if (!skipGlobalMerge) {
      //        this.globalMerge
      //      }
      //      this.applyToEachDagNode(n => if (n.transitions == null) throw new RuntimeException("null trans"))

      //      this.getNodesByLevel().foreach(l => println(l.size))
      //      var levels: List[mutable.Set[AutomatonNode]] = Nil
      val mniSet = ConcurrentHashMap.newKeySet[Integer](threshold * 2)
      this.MNISupportCached = Int.MaxValue
      if (this.parallelValidation) {

        val consumedBuffer: ThreadLocal[IntIntMap] = new ThreadLocal[IntIntMap] {
          override protected def initialValue(): IntIntMap = {
            AutomaTreeNodeFilter.instantiateIntIntMap(nbNodes - 1)
          }
        }

        val knownBuffer: ThreadLocal[IntIntMap] = new ThreadLocal[IntIntMap] {
          override protected def initialValue(): IntIntMap = {
            AutomaTreeNodeFilter.instantiateIntIntMap(nbNodes - 1)
          }
        }

        val blockedPathsV: ThreadLocal[IntIntMap] = new ThreadLocal[IntIntMap] {
          override protected def initialValue(): IntIntMap = {
            AutomaTreeNodeFilter.instantiateIntIntMap
          }
        }
        val blockedPathsT: ThreadLocal[IntIntMap] = new ThreadLocal[IntIntMap] {
          override protected def initialValue(): IntIntMap = {
            AutomaTreeNodeFilter.instantiateIntIntMap
          }
        }
        val releasePathsValue: ThreadLocal[Array[IntSet]] = new ThreadLocal[Array[IntSet]] {
          override protected def initialValue(): Array[IntSet] = {
            Array.fill(nbNodes + 1)(AutomaTreeNodeFilter.instantiateIntSet)
          }
        }
        val releasePathsTarget: ThreadLocal[Array[IntSet]] = new ThreadLocal[Array[IntSet]] {
          override protected def initialValue(): Array[IntSet] = {
            Array.fill(nbNodes + 1)(AutomaTreeNodeFilter.instantiateIntSet)
          }
        }
        val lockLevelsValue: ThreadLocal[Array[TreeSet[Int]]] = new ThreadLocal[Array[TreeSet[Int]]] {
          override protected def initialValue(): Array[TreeSet[Int]] = {
            Array.fill(nbNodes)(new TreeSet)
          }
        }
        val lockLevelsTarget: ThreadLocal[Array[TreeSet[Int]]] = new ThreadLocal[Array[TreeSet[Int]]] {
          override protected def initialValue(): Array[TreeSet[Int]] = {
            Array.fill(nbNodes)(new TreeSet)
          }
        }
        val filters: ThreadLocal[HashIntObjMap[List[(Int, fr.liglab.sami.utilities.Filter)]]] = new ThreadLocal[HashIntObjMap[List[(Int, fr.liglab.sami.utilities.Filter)]]] {
          override protected def initialValue(): HashIntObjMap[List[(Int, fr.liglab.sami.utilities.Filter)]] = {
            AutomaTreeNodeFilter.instantiateIntObjectMap[List[(Int, fr.liglab.sami.utilities.Filter)]](nbNodes)
          }
        }
        val filtersBuff: ThreadLocal[HashIntObjMap[List[(Int, fr.liglab.sami.utilities.Filter)]]] = new ThreadLocal[HashIntObjMap[List[(Int, fr.liglab.sami.utilities.Filter)]]] {
          override protected def initialValue(): HashIntObjMap[List[(Int, fr.liglab.sami.utilities.Filter)]] = {
            AutomaTreeNodeFilter.instantiateIntObjectMap[List[(Int, fr.liglab.sami.utilities.Filter)]](nbNodes)
          }
        }
        val pathByNodeId: ThreadLocal[IntIntMap] = new ThreadLocal[IntIntMap] {
          override protected def initialValue(): IntIntMap = {
            AutomaTreeNodeFilter.instantiateIntIntMap(nbNodes)
          }
        }
        val pathByNodeIdBuff: ThreadLocal[IntIntMap] = new ThreadLocal[IntIntMap] {
          override protected def initialValue(): IntIntMap = {
            AutomaTreeNodeFilter.instantiateIntIntMap(nbNodes)
          }
        }
        //        println("parallel validation")
        def recValidate(toDo: List[mutable.Set[AutomatonNode]], firstExec: Boolean): Boolean = {
          if (toDo.isEmpty) return true
          //          val sw = new StopWatch
          //          sw.start
          val done = toDo.head
          val currentLevel = done.head.codeVertexId
          val nextGen: ArrayBuffer[mutable.Set[AutomatonNode]] = new ArrayBuffer()
          //          if (!firstExec) levels = done :: levels
          if (!lazyCleanup || !automorph(currentLevel).isEmpty) {
            if (firstExec && done.head.successors.size == 1) {
              done.foreach(_.validateVerticesRootParallel(mniSet, consumedBuffer, knownBuffer, blockedPathsV, blockedPathsT, releasePathsValue, releasePathsTarget, lockLevelsValue, lockLevelsTarget, filters, filtersBuff, pathByNodeId, pathByNodeIdBuff))
            } else {
              done.par.foreach(n => {
                n.validateVertices(mniSet, consumedBuffer.get, knownBuffer.get, blockedPathsV.get, blockedPathsT.get, releasePathsValue.get, releasePathsTarget.get, lockLevelsValue.get, lockLevelsTarget.get, filters.get, filtersBuff.get, pathByNodeId.get, pathByNodeIdBuff.get, true /*lazyCleanup*/ )
              })
            }
            //            println(sw.getElapsedTime() + "\t" + done.head.codeVertexId + " level validated" + (if (lazyCleanup) " lazy" else ""))
            for (l <- automorph(currentLevel) if l != currentLevel) {
              this.nodesByDepth(l).par.foreach(_.filterTransitions(mniSet))
              //              println("filtering level " + l)
            }
            //            println(sw.getElapsedTime() + "\tfiltering done")
            this.MNISupportCached = Math.min(this.MNISupportCached, mniSet.size())
            if (this.MNISupportCached < threshold) {
              this.MNISupportCached = 0
              this.root = null
              return false
            }
          }
          for (n <- done) {
            if (firstExec && n.transitions.isEmpty()) {
              this.root = null
              return false
            } else if (n.transitions != null) {
              n.selectSuccessorsToValidate(nextGen)
            }
          }
          mniSet.clear()
          return recValidate(nextGen.toList ::: toDo.tail, false)
        }
        if (!recValidate(List(mutable.Set(this.root)), true)) return
      } else {
        val consumedBuffer: IntIntMap = AutomaTreeNodeFilter.instantiateIntIntMap(nbNodes - 1)
        val konwnBuffer: IntIntMap = AutomaTreeNodeFilter.instantiateIntIntMap(nbNodes - 1)
        val blockedPathsV: IntIntMap = AutomaTreeNodeFilter.instantiateIntIntMap
        val blockedPathsT: IntIntMap = AutomaTreeNodeFilter.instantiateIntIntMap
        val releasePathsValue: Array[IntSet] = Array.fill(nbNodes + 1)(AutomaTreeNodeFilter.instantiateIntSet)
        val releasePathsTarget: Array[IntSet] = Array.fill(nbNodes + 1)(AutomaTreeNodeFilter.instantiateIntSet)
        val lockLevelsValue: Array[TreeSet[Int]] = Array.fill(nbNodes)(new TreeSet)
        val lockLevelsTarget: Array[TreeSet[Int]] = Array.fill(nbNodes)(new TreeSet)
        val filters: HashIntObjMap[List[(Int, fr.liglab.sami.utilities.Filter)]] = AutomaTreeNodeFilter.instantiateIntObjectMap[List[(Int, fr.liglab.sami.utilities.Filter)]](this.nbNodes)
        val filtersBuff: HashIntObjMap[List[(Int, fr.liglab.sami.utilities.Filter)]] = AutomaTreeNodeFilter.instantiateIntObjectMap[List[(Int, fr.liglab.sami.utilities.Filter)]](this.nbNodes)
        val pathByNodeId: IntIntMap = AutomaTreeNodeFilter.instantiateIntIntMap(nbNodes)
        val pathByNodeIdBuff: IntIntMap = AutomaTreeNodeFilter.instantiateIntIntMap(nbNodes)
        def recValidate(toDo: List[mutable.Set[AutomatonNode]], firstExec: Boolean): Boolean = {
          if (toDo.isEmpty) return true
          val done = toDo.head
          val currentLevel = done.head.codeVertexId
          val nextGen: ArrayBuffer[mutable.Set[AutomatonNode]] = new ArrayBuffer()
          //          if (!firstExec) levels = done :: levels
          if (!lazyCleanup || !automorph(currentLevel).isEmpty) {
            for (n <- done) {
              //            val sw = new StopWatch
              //            sw.start
              //            val initNbTrans = n.transitions.size()
              n.validateVertices(mniSet, consumedBuffer, konwnBuffer, blockedPathsV, blockedPathsT, releasePathsValue, releasePathsTarget, lockLevelsValue, lockLevelsTarget, filters, filtersBuff, pathByNodeId, pathByNodeIdBuff, lazyCleanup)
              //            if (sw.getElapsedTime() > 100) println(sw.getElapsedTime() + "\t" + n + "\t" + initNbTrans + "-" + n.transitions.size())
            }
            for (l <- automorph(currentLevel) if l != currentLevel) {
              this.nodesByDepth(l).par.foreach(_.filterTransitions(mniSet))
            }
            this.MNISupportCached = Math.min(this.MNISupportCached, mniSet.size())
            if (this.MNISupportCached < threshold) {
              this.MNISupportCached = 0
              this.root = null
              return false
            }
          }
          for (n <- done) {
            if (firstExec && n.transitions.isEmpty()) {
              this.root = null
              return false
            } else if (n.transitions != null) {
              n.selectSuccessorsToValidate(nextGen)
            }
          }
          mniSet.clear()
          return recValidate(nextGen.toList ::: toDo.tail, false)
        }
        if (!recValidate(List(mutable.Set(this.root)), true)) return
      }
      //      println("second minimization " + System.currentTimeMillis())

      //      this.applyToEachDagNode(n => if (n.transitions == null) throw new RuntimeException("null trans"))

      //      while (!levels.isEmpty) {
      //        this.mergeLevel(levels.head)
      //        levels = levels.tail
      //      }
      this.cleanup

      //      this.applyToEachDagNode(n => if (n.transitions == null) throw new RuntimeException("null trans"))

    }
  }

  //  def newNode: AutomatonNode = {
  //    new AutomatonNode(HashIntObjMaps.newMutableMap())
  //  }

  def newNode(codeVertexId: Int, childPos: Int, m: IntObjMap[_], n: ArrayBuffer[AutomatonNode]): AutomatonNode = {
    new AutomatonNode(codeVertexId, childPos, m.keySet(), n)
  }

  def newNode(codeVertexId: Int, childPos: Int, m: Iterable[Int], n: ArrayBuffer[AutomatonNode]): AutomatonNode = {
    new AutomatonNode(codeVertexId, childPos, m, n)
  }

  def newNode(codeVertexId: Int, childPos: Int, s: IntSet, n: ArrayBuffer[AutomatonNode]): AutomatonNode = {
    new AutomatonNode(codeVertexId, childPos, s, n)
  }

  def newLeafNode(codeVertexId: Int, childPos: Int, s: IntSet): AutomatonNode = {
    newNode(codeVertexId, childPos, s, null)
  }

  def newDummyNode: AutomatonNode = {
    new AutomatonNode(-1, -1, null)
  }

  def newNode(codeVertexId: Int, childPos: Int, m: IntObjMap[ArrayBuffer[AutomatonNode]]): AutomatonNode = {
    new AutomatonNode(codeVertexId, childPos, m)
  }

  //  def mergeLevel(level: Iterable[AutomatonNode]) = {
  //    //if it has a single parent try to merge more?
  //    //    println("level " + level)
  //    val groupedBySizeAndNbSuc: LongObjMap[List[AutomatonNode]] = HashLongObjMaps.newMutableMap()
  //    for (n <- level) {
  //      if (n.transitions != null) {
  //        val key: Long = if (n.successors == null) {
  //          n.transitions.size.longValue()
  //        } else {
  //          (n.transitions.size.longValue() << 32) | (n.successors.size.longValue() & 0xffffffffL)
  //        }
  //        val l = groupedBySizeAndNbSuc.get(key)
  //        if (l == null) {
  //          groupedBySizeAndNbSuc.put(key, List(n))
  //        } else {
  //          groupedBySizeAndNbSuc.put(key, n :: l)
  //        }
  //      }
  //    }
  //    val c = groupedBySizeAndNbSuc.cursor()
  //    while (c.moveNext()) {
  //      val l = c.value()
  //      if (!l.tail.isEmpty) {
  //        //ObjObjMap was slow when there were many nodes
  //        val collisionMap: UnifiedMap[IntObjMap[ArrayBuffer[AutomatonNode]], AutomatonNode] = new UnifiedMap(l.size)
  //        for (n <- l) {
  //          val existing = collisionMap.putIfAbsent(n.transitions, n)
  //          if (existing != null) {
  //            //            println("merge")
  //            mergeNodes(n, existing)
  //          } else {
  //            //            println("no merge")
  //          }
  //        }
  //      }
  //    }
  //  }

  //  private final def mergeNodes(n: AutomatonNode, existing: AutomatonNode) = {
  //    //    println("merge " + n + " into " + existing + " " + n.codeVertexId + " " + existing.codeVertexId)
  //    val predCursor = n.predecessors.cursor()
  //    //    println(n.predecessors)
  //    while (predCursor.moveNext) {
  //      //      println(predCursor.key().successors(n.childPos).get(n))
  //      val pred = predCursor.key()
  //      val c = predCursor.value().intIterator()
  //      while (c.hasNext()) {
  //        val currentTrans = pred.transitions.get(c.next())
  //        currentTrans(n.childPos) = existing
  //      }
  //      val t = pred.successors(n.childPos).remove(n)
  //      if (pred.successors(n.childPos).containsKey(existing)) {
  //        pred.successors(n.childPos).get(existing).addAll(t)
  //        existing.predecessors.get(pred).addAll(t)
  //      } else {
  //        pred.successors(n.childPos).put(existing, t)
  //        existing.predecessors.put(pred, t)
  //      }
  //    }
  //    if (n.successors != null) {
  //      for (sucL <- n.successors) {
  //        val sucCursor = sucL.cursor()
  //        while (sucCursor.moveNext()) {
  //          sucCursor.key().predecessors.remove(n)
  //        }
  //      }
  //    }
  //    n.eraseData()
  //  }

  @SerialVersionUID(1L)
  class AutomatonNode(var codeVertexId: Int, var childPos: Int, var transitions: IntObjMap[ArrayBuffer[AutomatonNode]]) extends Serializable {
    var id: Int = getNextNodeId
    //    if (id > MAX_21_BITS) throw new RuntimeException("id too high, we only have 21 bits")
    if (transitions != null) {
      if (transitions.isEmpty()) throw new RuntimeException("building node with empty trans")
      transitions.shrink()
    }
    var successors: IndexedSeq[ObjObjMap[AutomatonNode, IntArrayList]] = null
    var predecessors: ObjObjMap[AutomatonNode, IntArrayList] = null
    //    (filteringLevel, allowedTransitions)
    var conditionalTransitions: List[(Int, fr.liglab.sami.utilities.Filter)] = Nil
    var backEdgeSource = false
    //    if(transitions == null) throw new RuntimeException("ho")

    //    def this(codeVertexId: Int, childPos: Int, m: IntObjMap[_], n: ArrayBuffer[AutomatonNode]) = {
    //      this(codeVertexId, childPos, HashIntObjMaps.getDefaultFactory[ArrayBuffer[AutomatonNode]].withHashConfig(AutomaTreeForwardRef.dsfsdfds).newMutableMap(m.size()))
    //      val cursor = m.cursor()
    //      while (cursor.moveNext()) {
    //        this.transitions.put(cursor.key(), n)
    //      }
    //    }

    def this(codeVertexId: Int, childPos: Int, s: IntSet, n: ArrayBuffer[AutomatonNode]) = {
      this(codeVertexId, childPos, new FakeMap(AutomaTreeNodeFilter.instantiateIntSet(s), n))
    }

    def this(codeVertexId: Int, childPos: Int, m: Iterable[Int], n: ArrayBuffer[AutomatonNode]) = {
      this(codeVertexId, childPos, new FakeMap(AutomaTreeNodeFilter.instantiateIntSet(m.size), n))
      for (k <- m) {
        this.transitions.put(k, n)
      }
    }

    //    def this(codeVertexId: Int, childPos: Int, m: IntObjMap[ArrayBuffer[AutomatonNode]]) {
    //      this(codeVertexId, childPos)
    //      this.transitions = m
    //    }

    override def equals(that: Any): Boolean =
      that match {
        case that: AutomatonNode => that.id == this.id
        case _                   => false
      }

    override def hashCode: Int = {
      return this.id * -1640531527
    }

    @throws(classOf[IOException])
    private def writeObject(out: ObjectOutputStream): Unit = {
      out.writeInt(this.id)
      out.writeInt(childPos)
      out.writeInt(codeVertexId)
      out.writeBoolean(backEdgeSource)
      if (transitions == null) {
        out.writeInt(0)
      } else {
        out.writeInt(transitions.size())
        out.writeBoolean(transitions.isInstanceOf[FakeMap[_]])
        val vCur = transitions.cursor()
        while (vCur.moveNext()) {
          out.writeInt(vCur.key())
          out.writeObject(vCur.value())
        }
      }
      out.writeObject(conditionalTransitions)
    }

    @throws(classOf[IOException])
    private def readObject(in: ObjectInputStream): Unit = {
      this.id = in.readInt()
      this.childPos = in.readInt()
      this.codeVertexId = in.readInt()
      this.backEdgeSource = in.readBoolean()
      val nbTrans = in.readInt()
      //      println("nb Trans " + nbTrans)
      if (nbTrans > 0) {
        if (in.readBoolean()) {
          //FIXME this only works with null leaves
          this.transitions = new FakeMap[ArrayBuffer[AutomatonNode]](AutomaTreeNodeFilter.instantiateIntSet(nbTrans), null)
        } else {
          this.transitions = AutomaTreeNodeFilter.instantiateIntObjectMap[ArrayBuffer[AutomatonNode]](nbTrans)
        }
        for (i <- 0 until nbTrans) {
          this.transitions.put(in.readInt(), in.readObject().asInstanceOf[ArrayBuffer[AutomatonNode]])
        }
        transitions.shrink()
      }
      conditionalTransitions =in.readObject.asInstanceOf[List[(Int, fr.liglab.sami.utilities.Filter)]]
    }

    override def toString(): String = {
      "n" + this.id + " (" + this.codeVertexId + "-" + backEdgeSource + ")"
    }
    //
    //    def addEdge(label: Int, n: AutomatonNode) = {
    //      this.transitions.put(label, n)
    //    }

    def eraseData() = {
      //      if (this.id == 115) {
      //        println("erasing " + this.id)
      //      }
      this.transitions = null
      this.successors = null
      this.predecessors = null
    }

    def filterTransitions(f: java.util.Set[Integer]) = {
      if (this.transitions != null) {
        if (this.successors == null) {
          this.transitions.keySet().retainAll(f)
        } else {
          val c = this.transitions.cursor()
          while (c.moveNext()) {
            if (!f.contains(c.key)) {
              for (n <- c.value()) {
                val transForSuc = successors(n.childPos).get(n)
                transForSuc.remove(c.key)
                if (transForSuc.isEmpty()) {
                  n.removePredecessor(this)
                  successors(n.childPos).remove(n)
                }
              }
              c.remove()
            }
          }
        }
        if (this.transitions.isEmpty()) {
          val c = this.predecessors.cursor()
          while (c.moveNext()) {
            c.key().removeSuccessor(this)
          }
          this.eraseData()
        }
      }
    }

    def validateVerticesRootParallel(distinctTrans: ConcurrentHashMap.KeySetView[Integer, java.lang.Boolean], knownPathBufferIn1: ThreadLocal[IntIntMap], knownPathBufferIn2: ThreadLocal[IntIntMap], blockedPathsV: ThreadLocal[IntIntMap], blockedPathsT: ThreadLocal[IntIntMap], releasePathsValue: ThreadLocal[Array[IntSet]], releasePathsTarget: ThreadLocal[Array[IntSet]], lockLevelsValue: ThreadLocal[Array[TreeSet[Int]]], lockLevelsTarget: ThreadLocal[Array[TreeSet[Int]]], filters: ThreadLocal[HashIntObjMap[List[(Int, fr.liglab.sami.utilities.Filter)]]], filtersBuffer: ThreadLocal[HashIntObjMap[List[(Int, fr.liglab.sami.utilities.Filter)]]], pathByNodeId: ThreadLocal[IntIntMap], pathByNodeIdBuffer: ThreadLocal[IntIntMap]) = {
      if (this.successors.size == 1) {
        def filtersCompatible(t: Int, listF: List[(Int, fr.liglab.sami.utilities.Filter)]): Boolean = {
          if (listF == null) return true
          for (f <- listF) {
            if (!f._2.compatible(t)) return false
          }
          return true
        }
        val sucCursor = successors(0).cursor()
        val sucList = new ArrayBuffer[(AutomatonNode, IntArrayList)](successors(0).size())
        while (sucCursor.moveNext()) { sucList.+=((sucCursor.key, sucCursor.value)) }
        sucList.par.foreach { x =>
          var knownPath: IntIntMap = null
          var knownFilters: HashIntObjMap[List[(Int, fr.liglab.sami.utilities.Filter)]] = null
          var knownById: IntIntMap = null
          var knownPathBuffer1 = knownPathBufferIn1.get
          var knownPathBuffer2 = knownPathBufferIn2.get
          var filters1 = filters.get
          var filters2 = filtersBuffer.get
          var pathById1 = pathByNodeId.get
          var pathById2 = pathByNodeIdBuffer.get
          val blockedPathsVg = blockedPathsV.get
          val blockedPathsTg = blockedPathsT.get
          val releasePathsValueg = releasePathsValue.get
          val releasePathsTargetg = releasePathsTarget.get
          val lockLevelsValueg = lockLevelsValue.get
          val lockLevelsTargetg = lockLevelsTarget.get
          val successor = x._1
          val vCur = x._2.intIterator()
          while (vCur.hasNext()) {
            val elem = vCur.next
            //known path only usable if same successor node
            //can't skip because of filters
            if (knownPath == null || knownPath.containsKey(elem) || !filtersCompatible(elem, knownFilters.get(this.codeVertexId))) {
              //            val sw = new StopWatch
              //            sw.start()
              val path = existsPath(elem, ArrayBuffer(successor), this, knownPathBuffer1, blockedPathsVg, blockedPathsTg, releasePathsValueg, releasePathsTargetg, lockLevelsValueg, lockLevelsTargetg, filters1, pathById1, false)
              //            if (sw.getElapsedTime() > 100) println(this + "\t" + elem + "\t" + sw.getElapsedTime() + "\t" + (path == null))
              if (path == null) {
                this.transitions.synchronized {
                  this.transitions.remove(elem)
                }
                vCur.remove()
              } else {
                knownPath = path
                val swap = knownPathBuffer1
                knownPathBuffer1 = knownPathBuffer2
                knownPathBuffer2 = swap
                knownFilters = filters1
                val swf = filters1
                filters1 = filters2
                filters2 = swf
                knownById = pathById1
                val swi = pathById1
                pathById1 = pathById2
                pathById2 = swi
              }
            }
          }
          if (x._2.isEmpty()) {
            x._1.removePredecessor(this)
            this.successors(0).synchronized {
              this.successors(0).remove(x._1)
            }
          }
        }
      } else {
        this.transitions.keySet().toIntArray().par.foreach {
          t =>
            {
              val dests = this.synchronized(transitions.get(t))
              val path = existsPath(t, dests, this, knownPathBufferIn1.get, blockedPathsV.get, blockedPathsT.get, releasePathsValue.get, releasePathsTarget.get, lockLevelsValue.get, lockLevelsTarget.get, filters.get, pathByNodeId.get, false)
              if (path == null) {
                this.successors.synchronized {
                  if (dests != null) {
                    for (i <- 0 until dests.size) {
                      val tr = this.successors(i).get(dests(i))
                      tr.remove(t)
                      if (tr.isEmpty()) {
                        this.successors(i).remove(dests(i))
                        dests(i).removePredecessor(this)
                      }
                    }
                  }
                }
                this.transitions.synchronized(this.transitions.remove(t))
              }
            }
        }
      }
      distinctTrans.addAll(this.transitions.keySet())
      this.transitions.shrink()
    }

    def validateVertices(distinctTrans: ConcurrentHashMap.KeySetView[Integer, java.lang.Boolean], knownPathBufferIn1: IntIntMap, knownPathBufferIn2: IntIntMap, blockedPathsV: IntIntMap, blockedPathsT: IntIntMap, releasePathsValue: Array[IntSet], releasePathsTarget: Array[IntSet], lockLevelsValue: Array[TreeSet[Int]], lockLevelsTarget: Array[TreeSet[Int]], filters: HashIntObjMap[List[(Int, fr.liglab.sami.utilities.Filter)]], filtersBuffer: HashIntObjMap[List[(Int, fr.liglab.sami.utilities.Filter)]], pathByNodeId: IntIntMap, pathByNodeIdBuffer: IntIntMap, lazyCleanup: Boolean) = {
      if (successors == null || successors.size == 1) {
        var knownPath: IntIntMap = null
        var knownFilters: HashIntObjMap[List[(Int, fr.liglab.sami.utilities.Filter)]] = null
        var knownById: IntIntMap = null
        def filtersCompatible(t: Int, listF: List[(Int, fr.liglab.sami.utilities.Filter)]): Boolean = {
          if (listF == null) return true
          for (f <- listF) {
            if (!f._2.compatible(t)) return false
          }
          return true
        }
        var knownPathBuffer1 = knownPathBufferIn1
        var knownPathBuffer2 = knownPathBufferIn2
        var filters1 = filters
        var filters2 = filtersBuffer
        var pathById1 = pathByNodeId
        var pathById2 = pathByNodeIdBuffer
        if (this.successors == null) {
          //last level
          val transitionsCursor = transitions.cursor()
          var nbDone = 0
          //          val allFilterSeen = HashIntSets.newMutableSet()
          while (transitionsCursor.moveNext()) {
            nbDone += 1
            //known path only usable if same successor node
            if (lazyCleanup && distinctTrans.contains(transitionsCursor.key())) {

            } else {
              if (knownPath == null || knownPath.containsKey(transitionsCursor.key()) || !filtersCompatible(transitionsCursor.key(), knownFilters.get(this.codeVertexId))) {
                //              val sw = new StopWatch
                //              sw.start()
                val path = existsPath(transitionsCursor.key(), transitionsCursor.value(), this, knownPathBuffer1, blockedPathsV, blockedPathsT, releasePathsValue, releasePathsTarget, lockLevelsValue, lockLevelsTarget, filters1, pathById1, false)
                //              if (sw.getElapsedTime() > 500) {
                //                println(this + "\t" + transitionsCursor.key + "\t" + sw.getElapsedTime() + "\t" + (path == null))
                //              }
                if (path == null) {
                  transitionsCursor.remove()
                } else {
                  //                  if (parallelValidation) {
                  //                    distinctTrans.synchronized {
                  //                      distinctTrans.add(transitionsCursor.key())
                  //                    }
                  //                  } else {
                  distinctTrans.add(transitionsCursor.key())
                  //                  }
                  knownPath = path
                  val swap = knownPathBuffer1
                  knownPathBuffer1 = knownPathBuffer2
                  knownPathBuffer2 = swap
                  knownFilters = filters1
                  val swf = filters1
                  filters1 = filters2
                  filters2 = swf
                  knownById = pathById1
                  val swi = pathById1
                  pathById1 = pathById2
                  pathById2 = swi
                }
              } else {
                //              for (i <- 0 until nbNodes) {
                //                if (i == this.codeVertexId)
                //                  print(transitionsCursor.key() + "\t")
                //                else
                //                  print(pathByNodeId.get(i) + "\t")
                //              }
                //              println()
                //                if (parallelValidation) {
                //                  distinctTrans.synchronized {
                //                    distinctTrans.add(transitionsCursor.key())
                //                  }
                //                } else {
                distinctTrans.add(transitionsCursor.key())
                //                }
              }
            }
          }
        } else {
          val sucCursor = successors(0).cursor()
          while (sucCursor.moveNext()) {
            var knownPath: IntIntMap = null
            var knownFilters: HashIntObjMap[List[(Int, fr.liglab.sami.utilities.Filter)]] = null
            var knownById: IntIntMap = null
            val successor = sucCursor.key()
            val vCur = sucCursor.value().intIterator()
            while (vCur.hasNext()) {
              val elem = vCur.next
              if (lazyCleanup && distinctTrans.contains(elem)) {

              } else {
                //known path only usable if same successor node
                //can't skip because of filters
                if (knownPath == null || knownPath.containsKey(elem) || !filtersCompatible(elem, knownFilters.get(this.codeVertexId))) {
                  val path = existsPath(elem, ArrayBuffer(successor), this, knownPathBuffer1, blockedPathsV, blockedPathsT, releasePathsValue, releasePathsTarget, lockLevelsValue, lockLevelsTarget, filters1, pathById1, false)
                  if (path == null) {
                    this.transitions.remove(elem)
                    vCur.remove()
                  } else {
                    //                    if (parallelValidation) {
                    //                      distinctTrans.synchronized {
                    //                        distinctTrans.add(elem)
                    //                      }
                    //                    } else {
                    distinctTrans.add(elem)
                    //                    }
                    knownPath = path
                    val swap = knownPathBuffer1
                    knownPathBuffer1 = knownPathBuffer2
                    knownPathBuffer2 = swap
                    knownFilters = filters1
                    val swf = filters1
                    filters1 = filters2
                    filters2 = swf
                    knownById = pathById1
                    val swi = pathById1
                    pathById1 = pathById2
                    pathById2 = swi
                  }
                } else {
                  //                for (i <- 0 until nbNodes) {
                  //                  if (i == this.codeVertexId)
                  //                    print(elem + "\t")
                  //                  else
                  //                    print(pathByNodeId.get(i) + "\t")
                  //                }
                  //                println()
                  //                  if (parallelValidation) {
                  //                    distinctTrans.synchronized {
                  //                      distinctTrans.add(elem)
                  //                    }
                  //                  } else {
                  distinctTrans.add(elem)
                  //                  }
                }
              }
            }
            if (sucCursor.value().isEmpty()) {
              sucCursor.key().removePredecessor(this)
              sucCursor.remove()
            }
          }
        }
      } else {
        val transitionsCursor = transitions.cursor()
        while (transitionsCursor.moveNext()) {
          if (lazyCleanup && distinctTrans.contains(transitionsCursor.key())) {

          } else {
            //          debug = false
            //          if (preDebug && transitionsCursor.key() == 2010081) {
            //            println("going in")
            //            debug = true
            //          }
            //          val sw = new StopWatch
            //          sw.start()
            val path = existsPath(transitionsCursor.key(), transitionsCursor.value(), this, knownPathBufferIn1, blockedPathsV, blockedPathsT, releasePathsValue, releasePathsTarget, lockLevelsValue, lockLevelsTarget, filters, pathByNodeId, false)
            //          if (sw.getElapsedTime() > 100) println(transitionsCursor.key() + "\t" + sw.getElapsedTime())
            //          if (debug) println(path)
            if (path == null) {
              if (transitionsCursor.value() != null) {
                for (i <- 0 until transitionsCursor.value().size) {
                  val t = this.successors(i).get(transitionsCursor.value()(i))
                  t.remove(transitionsCursor.key)
                  if (t.isEmpty()) {
                    this.successors(i).remove(transitionsCursor.value()(i))
                    transitionsCursor.value()(i).removePredecessor(this)
                  }
                }
              }
              transitionsCursor.remove()
            } else {
              //              if (parallelValidation) {
              //                distinctTrans.synchronized {
              //                  distinctTrans.add(transitionsCursor.key())
              //                }
              //              } else {
              distinctTrans.add(transitionsCursor.key())
              //              }
            }
          }
        }
      }
      if (!lazyCleanup && this.transitions.isEmpty() && this != root) {
        throw new Exception("impossible, parent checked " + this + " parents " + this.predecessors)
      } else if (this.transitions.isEmpty()) {
        if (this.predecessors != null) {
          val predC = this.predecessors.cursor()
          while (predC.moveNext()) {
            predC.key.removeSuccessor(this)
          }
        }
        this.eraseData()
      } else {
        this.transitions.shrink()
      }
      //      if (sw.getElapsedTime() > 10000) {
      //      println(this + "\t" + initialNbTrans + "\t" + this.transitions.size() + "\t" + sw.getElapsedTime())
      //      }
      //      if(debug) println("exiting debug")
      //      debug = false
      //      preDebug = false
    }

    def removePredecessor(node: AutomatonNode): Unit = {
      if (parallelValidation) {
        this.synchronized {
          this.predecessors.remove(node)
          if (this.predecessors.isEmpty()) {
            if (this.successors != null) {
              for (successorType <- this.successors) {
                val c = successorType.cursor()
                while (c.moveNext()) {
                  c.key().removePredecessor(this)
                }
              }
            }
            this.eraseData()
          }
        }
      } else {
        this.predecessors.remove(node)
        if (this.predecessors.isEmpty()) {
          if (this.successors != null) {
            for (successorType <- this.successors) {
              val c = successorType.cursor()
              while (c.moveNext()) {
                c.key().removePredecessor(this)
              }
            }
          }
          this.eraseData()
        }
      }
    }

    def removeSuccessor(node: AutomatonNode): Unit = {
      if (parallelValidation) {
        this.synchronized {
          val eliminatedTrans = this.successors(node.childPos).remove(node)
          val elimTC = eliminatedTrans.intIterator()
          while (elimTC.hasNext()) {
            val t = elimTC.next()
            if (this.successors.size != 1) {
              val impacted = this.transitions.get(t)
              for (s <- impacted if s.childPos != node.childPos) {
                val updatedList = successors(s.childPos).get(s)
                updatedList.remove(t)
                this.transitions.remove(t)
                if (updatedList.isEmpty()) {
                  successors(s.childPos).remove(s)
                  s.removePredecessor(this)
                }
              }
            }
            this.transitions.remove(t)
          }
          if (this.transitions.isEmpty()) {
            if (this.predecessors != null) {
              val predC = this.predecessors.cursor()
              while (predC.moveNext()) {
                predC.key.removeSuccessor(this)
              }
            }
            this.eraseData()
          }
        }
      } else {
        val eliminatedTrans = this.successors(node.childPos).remove(node)
        val elimTC = eliminatedTrans.intIterator()
        while (elimTC.hasNext()) {
          val t = elimTC.next()
          if (this.successors.size != 1) {
            val impacted = this.transitions.get(t)
            for (s <- impacted if s.childPos != node.childPos) {
              val updatedList = successors(s.childPos).get(s)
              updatedList.remove(t)
              this.transitions.remove(t)
              if (updatedList.isEmpty()) {
                successors(s.childPos).remove(s)
                s.removePredecessor(this)
              }
            }
          }
          this.transitions.remove(t)
        }
        if (this.transitions.isEmpty()) {
          if (this.predecessors != null) {
            val predC = this.predecessors.cursor()
            while (predC.moveNext()) {
              predC.key.removeSuccessor(this)
            }
          }
          this.eraseData()
        }
      }
    }

    def prepFilters: Unit = {
      def mergeFilters(l: List[(Int, fr.liglab.sami.utilities.Filter)], f: fr.liglab.sami.utilities.Filter, p: Int, singlePred: Boolean): List[(Int, fr.liglab.sami.utilities.Filter)] = {
        val existing = l.find(_._1 == p)
        if (existing.isEmpty) {
          if (singlePred) {
            return (p, f) :: l
          } else {
            val newFilter = new ProbabilisticFilter(BFInstantiator.instantiateBF())
            if (!newFilter.merge(f)) {
              throw new RuntimeException("merging failed!1")
            }
            return (p, newFilter) :: l
          }
        } else {
          val e = existing.get
          if (!e._2.isExact && !e._2.merge(f)) {
            throw new RuntimeException("merging failed!2")
          }
          return l
        }
      }
      if (this.predecessors != null) {
        val predCur = this.predecessors.cursor()
        val singlePred = this.predecessors.size == 1
        while (predCur.moveNext()) {
          if (predCur.key.conditionalTransitions.isEmpty) {
            return
          }
          for (f <- predCur.key.conditionalTransitions) {
            this.conditionalTransitions = mergeFilters(this.conditionalTransitions, f._2, f._1, singlePred)
          }
        }
      }
    }

    def prepSuccessorsAndPredecessors: Unit = {
      if (this != root) {
        this.predecessors = AutomaTreeNodeFilter.instantiateObjectObjectMap[AutomatonNode, IntArrayList]
      }
      val transCursor = transitions.cursor()
      while (transCursor.moveNext()) {
        val nodes = transCursor.value()
        if (nodes == null) {
          return
        } else {
          for (i <- 0 until nodes.length) {
            if (this.successors == null) {
              this.successors = ArrayBuffer.fill(nodes.length)(AutomaTreeNodeFilter.instantiateObjectObjectMap[AutomatonNode, IntArrayList])
            }
            var t = this.successors(i).get(nodes(i))
            if (t == null) {
              t = new IntArrayList
              this.successors(i).put(nodes(i), t)
            }
            t.add(transCursor.key())
          }
        }
      }
      for (successor <- this.successors) {
        successor.shrink()
        val sucCursor = successor.cursor()
        while (sucCursor.moveNext()) {
          sucCursor.value.trimToSize()
          if (parallelValidation) {
            sucCursor.key().synchronized {
              sucCursor.key().predecessors.put(this, sucCursor.value())
            }
          } else {
            sucCursor.key().predecessors.put(this, sucCursor.value())
          }
        }
      }
    }

    def deleteSuccessorsAndPredecessors() = {
      this.successors = null
      this.predecessors = null
      this.conditionalTransitions = this.conditionalTransitions.filter(_._2.isExact)
    }

    def selectSuccessorsToValidate(r: ArrayBuffer[mutable.Set[AutomatonNode]]) = {
      if (this.successors != null) {
        for (i <- 0 until successors.size) {
          val c = successors(i).cursor()
          if (r.size == i) {
            r += mutable.HashSet.empty
          }
          while (c.moveNext()) {
            //            if (c.key().transitions == null) throw new RuntimeException("bad!")
            //        if (!c.value().isEmpty()) {

            r(i).add(c.key())
            //        }
          }
        }
      }
    }

    private def existsPath(v: Int, outTrans: ArrayBuffer[AutomatonNode], source: AutomatonNode, consumed: IntIntMap, blockedForTarget: IntIntMap, blockedForValue: IntIntMap, releasePathsTransValueChange: Array[IntSet], releasePathsTransTargetChange: Array[IntSet], lockLevelsForValue: Array[TreeSet[Int]], lockLevelsForTarget: Array[TreeSet[Int]], filters: HashIntObjMap[List[(Int, fr.liglab.sami.utilities.Filter)]], pathByNodeId: IntIntMap, debug: Boolean): IntIntMap = {
      def addFilters(listF: List[(Int, fr.liglab.sami.utilities.Filter)], step: Int, goingUp: Boolean) = {
        //        if (debug) println("add filter " + step + "\t" + listF)
        for (f <- listF) {
          if (f._2.isExact) {
            val existing = filters.get(f._1)
            if (existing == null) {
              filters.put(f._1, List((step, f._2)))
            } else {
              filters.put(f._1, existing.filter(_._2.isExact).+:((step, f._2)))
            }
          } else {
            val existing = filters.get(f._1)
            if (existing == null) {
              filters.put(f._1, List((step, f._2)))
            } else if (goingUp && !existing.head._2.isExact) {
              filters.put(f._1, List((step, f._2)))
            }
          }
        }
      }
      def removeFilters(listF: List[(Int, fr.liglab.sami.utilities.Filter)], step: Int) = {
        for (f <- listF) {
          val existing = filters.get(f._1)
          if (existing != null) {
            val updated = existing.filter(_._1 != step)
            if (updated.isEmpty) {
              filters.remove(f._1)
            } else {
              filters.put(f._1, updated)
            }
          }
        }
      }
      def checkFiltersConsistency(listF: List[(Int, fr.liglab.sami.utilities.Filter)]): Int = {
        var minStep = -1
        for (f <- listF) {
          val present = pathByNodeId.get(f._1)
          if (present != -1 && !f._2.compatible(present)) {
            if (minStep == -1) {
              minStep = consumed.get(present)
              if (minStep == -1) throw new RuntimeException("wtf")
            } else {
              val alt = consumed.get(present)
              if (alt == -1) throw new RuntimeException("wtf")
              minStep = Math.min(minStep, alt)
            }
          }
        }
        //        if (!listF.isEmpty) println(listF + "\t" +pathByNodeId + "\t"+ consumed+ "\t"+ minStep)
        return minStep
      }
      def applyFilters(elem: Int, listF: List[(Int, fr.liglab.sami.utilities.Filter)]): Int = {
        //        if(debug) println("applying filters " + elem + "\t" + listF)
        if (listF == null) return -1
        for (f <- listF) {
          if (!f._2.compatible(elem)) {
            return f._1
          }
        }
        return -1
      }
      def releaseForValue(step: Int) = {
        for (unlockPos <- step until releasePathsTransValueChange.size) {
          if (!releasePathsTransValueChange(unlockPos).isEmpty()) {
            val c = releasePathsTransValueChange(unlockPos).cursor()
            while (c.moveNext()) {
              blockedForValue.remove(c.elem())
              blockedForTarget.remove(c.elem())
              c.remove()
            }
          }
        }
      }
      def releaseForTarget(step: Int) = {
        for (unlockPos <- step until releasePathsTransTargetChange.size) {
          if (!releasePathsTransTargetChange(unlockPos).isEmpty()) {
            val c = releasePathsTransTargetChange(unlockPos).cursor()
            while (c.moveNext()) {
              blockedForValue.remove(c.elem())
              blockedForTarget.remove(c.elem())
              c.remove()
            }
          }
        }
      }
      consumed.clear()
      blockedForValue.clear()
      blockedForTarget.clear()
      for (s <- releasePathsTransValueChange) {
        s.clear()
      }
      for (s <- releasePathsTransTargetChange) {
        s.clear()
      }
      filters.clear
      pathByNodeId.clear
      consumed.put(v, 0)
      pathByNodeId.put(source.codeVertexId, v)
      //min added by reached, recursion added by
      val previousValueChoices = Array.fill(nbNodes)(-1)
      val previousTargetChoices = Array.fill(nbNodes)(-1)
      def recExistsPath(toDoDown: List[(Int, AutomatonNode)], toDoUp: (Int, AutomatonNode), step: Int): (Int, Int) = {
        //        if(debug) println("recursion " + step)
        if (debug) {
          println(step + "\t" + toDoDown + "\t" + toDoUp + "\t" + consumed + "\t" + pathByNodeId + "\t" + blockedForValue + "\t" + blockedForTarget)
        }
        if (toDoDown.isEmpty && (toDoUp == null || toDoUp._2.predecessors == null)) return (-1, -1)
        lockLevelsForValue(step).clear()
        lockLevelsForTarget(step).clear()
        var nextStep: AutomatonNode = null
        var locksGoingTo: Int = -1
        var lowestBacktrackPoint = step
        if (toDoDown.isEmpty) {
          //        if (toDoUp != null && toDoUp._2.predecessors != null) {
          nextStep = toDoUp._2
          locksGoingTo = toDoUp._1
          if (debug) println(step + " trying to go up " + nextStep.predecessors)
          val suitablePredecessor = nextStep.predecessors.cursor()
          while (suitablePredecessor.moveNext()) {
            val nextToDoUp = suitablePredecessor.key()
            if (debug) println(step + " trying to go up with " + nextToDoUp)
            var bsV = blockedForValue.get(nextToDoUp.id)
            var bsT = blockedForTarget.get(nextToDoUp.id)
            var bannedTransition = if (bsV == step) {
              bsV = -1
              previousValueChoices(step)
            } else {
              -1
            }
            if (bsV == -1 && bsT == -1) {
              val filtersConsistency = checkFiltersConsistency(nextToDoUp.conditionalTransitions)
              if (filtersConsistency == -1) {
                addFilters(nextToDoUp.conditionalTransitions, step, true)
                val filtersToApply = filters.get(nextToDoUp.codeVertexId)
                if (debug) println(step + " trying to go up with " + nextToDoUp + " - " + suitablePredecessor.value)
                if (nextToDoUp.successors.size == 1) {
                  if (previousTargetChoices(step) == -1) {
                    previousTargetChoices(step) = nextToDoUp.id
                  } else if (nextToDoUp.id != previousValueChoices(step)) {
                    previousTargetChoices(step) = nextToDoUp.id
                    releaseForTarget(step)
                    if (debug) println(step + "target unlock>= " + step)
                  } else {
                    if (debug) println(step + " no target unlock")
                  }
                  if (!nextToDoUp.backEdgeSource && suitablePredecessor.value.size >= nbNodes) {
                    val recRes = recExistsPath(toDoDown, (step, nextToDoUp), step + 1)
                    if (debug) println(step + " rec res " + recRes)
                    if (recRes._1 == -1) {
                      return (-1, -1)
                    } else {
                      lowestBacktrackPoint = Math.min(recRes._1, lowestBacktrackPoint)
                    }
                  } else {
                    val transIter = suitablePredecessor.value().intIterator()
                    var fastExit = false
                    while (transIter.hasNext() && !fastExit) {
                      val elem = transIter.next
                      if (debug) println(step + "\t" + elem + "\t" + previousValueChoices(step))
                      //the node may be locked but if we find a different transition to it then it's fine
                      if (elem != bannedTransition) {
                        if (previousValueChoices(step) == -1) {
                          previousValueChoices(step) = elem
                        } else if (elem != previousValueChoices(step)) {
                          previousValueChoices(step) = elem
                          releaseForValue(step)
                          if (debug) println(step + " unlock>= " + step)
                        } else {
                          if (debug) println(step + " no unlock")
                        }
                        val filterTest = applyFilters(elem, filtersToApply)
                        if (filterTest == -1) {
                          val putRes = consumed.putIfAbsent(elem, step)
                          if (putRes == -1) {
                            pathByNodeId.put(nextToDoUp.codeVertexId, elem)
                            val recRes = recExistsPath(toDoDown, (step, nextToDoUp), step + 1)
                            if (debug) println(step + " rec res " + recRes)
                            if (recRes._1 == -1) {
                              return (-1, -1)
                            } else {
                              lowestBacktrackPoint = Math.min(recRes._1, lowestBacktrackPoint)
                              //no need to check locklevelsfortarget, there is no queue down and we re not adding anything there
                              //                              if ((lockLevelsForValue(step + 1).isEmpty() || lockLevelsForValue(step + 1).last() < step)) {
                              //                                fastExit = true
                              //                                if (debug) println(step + " taking fast exit")
                              //                              }
                            }
                            consumed.remove(elem)
                            pathByNodeId.remove(nextToDoUp.codeVertexId)
                          } else {
                            lockLevelsForValue(step).add(putRes)
                          }
                        } else {
                          if (filterTest == step) lockLevelsForValue(step).add(0) else lockLevelsForTarget(step).add(filterTest)
                          if (debug) println("failed on filterTest " + filterTest + " " + filtersToApply)
                        }
                      }
                    }
                  }
                } else {
                  //multiple targets, need to release for target as well
                  val transIter = suitablePredecessor.value().intIterator()
                  while (transIter.hasNext()) {
                    val elem = transIter.next
                    if (debug) println(step + "\t" + elem + "\t" + previousValueChoices(step))
                    if (elem != bannedTransition) {
                      if (previousValueChoices(step) == -1) {
                        previousValueChoices(step) = elem
                      } else if (elem != previousValueChoices(step)) {
                        previousValueChoices(step) = elem
                        releaseForValue(step)
                        if (debug) println(step + " unlock>= " + step)
                      } else {
                        if (debug) println(step + " no unlock")
                      }
                      val putRes = consumed.putIfAbsent(elem, step)
                      if (putRes == -1) {
                        pathByNodeId.put(nextToDoUp.codeVertexId, elem)
                        var nextToDoDown: List[(Int, AutomatonNode)] = Nil
                        val successors = nextToDoUp.transitions.get(elem)
                        if (successors != null) {
                          for (i <- 0 until successors.size) {
                            if (i != nextStep.childPos) {
                              nextToDoDown = (step, successors(i)) :: nextToDoDown
                            }
                          }
                        }
                        var downFiltersConsistency = -1
                        for (n <- nextToDoDown) {
                          val l = checkFiltersConsistency(n._2.conditionalTransitions)
                          if (l != -1 && (downFiltersConsistency == -1 || l < filtersConsistency)) {
                            downFiltersConsistency = l
                          }
                        }
                        if (downFiltersConsistency == -1) {
                          for (n <- nextToDoDown) {
                            addFilters(n._2.conditionalTransitions, step, false)
                          }
                          val filterTest = applyFilters(elem, filtersToApply)
                          if (filterTest == -1) {
                            val recRes = recExistsPath(nextToDoDown, (step, nextToDoUp), step + 1)
                            if (debug) println(step + " rec res " + recRes)
                            if (recRes._1 == -1) {
                              return (-1, -1)
                            } else {
                              releaseForTarget(step)
                              lowestBacktrackPoint = Math.min(recRes._1, lowestBacktrackPoint)
                              for (n <- nextToDoDown) {
                                removeFilters(n._2.conditionalTransitions, step)
                              }
                            }
                          } else {
                            if (filterTest == step) lockLevelsForValue(step).add(0) else lockLevelsForTarget(step).add(filterTest)
                          }
                        } else {
                          lockLevelsForValue(step).add(downFiltersConsistency)
                        }
                        consumed.remove(elem)
                        pathByNodeId.remove(suitablePredecessor.key().codeVertexId)
                      } else {
                        lockLevelsForValue(step).add(putRes)
                      }
                    }
                  }
                }
                removeFilters(nextToDoUp.conditionalTransitions, step)
                //                //TODO THIS IS THE PROBLEMATIC ONE
                //                if (!nextToDoUp.conditionalTransitions.isEmpty) {
                //                  releaseForValue(step)
                //                }
              } else {
                if (debug) println("failed due to filter consistency " + nextToDoUp.conditionalTransitions)
                lockLevelsForValue(step).add(filtersConsistency)
              }
            } else {
              if (debug) println("failed due to blocked status " + bsV + "-" + bsT)
              if (bsT != -1 && bsT != step) {
                lockLevelsForTarget(step).add(bsT)
              }
              if (bsV != -1 && bsV != step) {
                lockLevelsForValue(step).add(bsV)
              }
            }
          }
        } else {
          nextStep = toDoDown.head._2
          locksGoingTo = toDoDown.head._1
          if (debug) println(step + " trying to go down with " + nextStep + "\t" + nextStep.transitions.keySet())
          if (nextStep.successors == null) {
            val filtersToApply = filters.get(nextStep.codeVertexId)
            if (!nextStep.backEdgeSource && nextStep.transitions.size >= nbNodes) {
              if (debug) println(step + " " + nextStep + " no successors and enough nodes")
              val recRes = recExistsPath(toDoDown.tail, toDoUp, step + 1)
              if (debug) println(step + " rec res " + recRes)
              if (recRes._1 == -1) {
                return (-1, -1)
              } else {
                lowestBacktrackPoint = Math.min(recRes._1, lowestBacktrackPoint)
              }
            } else {
              if (debug) println(step + " " + nextStep + " no successors")
              val nextTrans = nextStep.transitions.cursor()
              var fastExit = false
              while (nextTrans.moveNext() && !fastExit) {
                var plannedBlockLevel = -1
                val blockUpV = blockedForValue.get(toDoUp._2.id)
                val blockUpT = blockedForTarget.get(toDoUp._2.id)
                if (blockUpV != -1 || blockUpT != -1) {
                  val level = if (blockUpT != -1) { Math.max(blockUpT, toDoUp._1) } else { toDoUp._1 }
                  plannedBlockLevel = level
                  if (level != step) lockLevelsForTarget(step).add(level)
                  if (blockUpV != -1) {
                    if (blockUpV != step) lockLevelsForValue(step).add(blockUpV)
                    plannedBlockLevel = Math.max(plannedBlockLevel, blockUpV)
                  }
                } else {
                  for (n <- toDoDown.tail) {
                    val blockDownV = blockedForValue.get(n._2.id)
                    val blockDownT = blockedForTarget.get(n._2.id)
                    if (blockDownV != -1 || blockDownT != -1) {
                      val level = if (blockDownT != -1) { Math.max(blockDownT, n._1) } else { n._1 }
                      if (plannedBlockLevel == -1 || level < plannedBlockLevel) {
                        plannedBlockLevel = level
                      }
                      if (level != step) lockLevelsForTarget(step).add(level)
                      if (blockDownV != -1) {
                        if (blockDownV != step) lockLevelsForValue(step).add(blockDownV)
                        plannedBlockLevel = Math.max(plannedBlockLevel, blockDownV)
                      }
                    }
                  }
                }
                if (plannedBlockLevel != -1) {
                  if (debug) println(step + " reseting locks (2) to " + plannedBlockLevel)
                  //                  if (plannedBlockLevel != step) {
                  lockLevelsForValue(step).tailSet(plannedBlockLevel + 1).clear()
                  lockLevelsForTarget(step).tailSet(plannedBlockLevel + 1).clear()
                  //                    lockLevelsForTarget(step).add(plannedBlockLevel)
                  //                  }
                  fastExit = true
                } else {
                  if (debug) println(step + " trying " + nextStep + " with " + nextTrans.key())
                  val filterTest = applyFilters(nextTrans.key(), filtersToApply)
                  if (filterTest == -1) {
                    val putRes = consumed.putIfAbsent(nextTrans.key(), step)
                    if (putRes == -1) {
                      pathByNodeId.put(nextStep.codeVertexId, nextTrans.key())
                      val recRes = recExistsPath(toDoDown.tail, toDoUp, step + 1)
                      if (debug) println(step + " rec res " + recRes)
                      if (recRes._1 == -1) {
                        return (-1, -1)
                      } else {
                        lowestBacktrackPoint = Math.min(recRes._1, lowestBacktrackPoint)
                        //no need to check for target, we do it on next iteration of while anyway
                        //                        if (lockLevelsForValue(step + 1).isEmpty() || lockLevelsForValue(step + 1).last() < step) {
                        //                          if (debug) println(step + " taking fast exit")
                        //                          fastExit = true
                        //                        }
                        if (debug) println(step + " release for value")
                        releaseForValue(step)
                      }
                      consumed.remove(nextTrans.key())
                      pathByNodeId.remove(nextStep.codeVertexId)
                    } else {
                      if (debug) println("failed on put")
                      lockLevelsForValue(step).add(putRes)
                    }
                  } else {
                    if (debug) println("failed on filter test")
                    if (filterTest == step) lockLevelsForValue(step).add(0) else lockLevelsForTarget(step).add(filterTest)
                  }
                }
              }
            }
          } else if (nextStep.successors.size == 1) {
            var superFastExit = false
            if (debug) println(step + " " + nextStep + " one successor")
            val sucCur = nextStep.successors(0).cursor()
            while (sucCur.moveNext() && !superFastExit) {
              val bsV = blockedForValue.get(sucCur.key.id)
              val bsT = blockedForTarget.get(sucCur.key.id)
              if (debug) println(step + " " + nextStep + " bs " + sucCur.key + "=" + bsV + "-" + bsT)
              if (bsV == -1 && bsT == -1) {
                val filtersConsistency = checkFiltersConsistency(sucCur.key().conditionalTransitions)
                if (filtersConsistency == -1) {
                  addFilters(sucCur.key().conditionalTransitions, step, false)
                  val filtersToApply = filters.get(nextStep.codeVertexId)
                  if (!nextStep.backEdgeSource && sucCur.value().size() >= nbNodes) {
                    if (debug) println(step + " " + nextStep + " by " + sucCur.key() + " enough nodes")
                    val recRes = recExistsPath((step, sucCur.key()) :: toDoDown.tail, toDoUp, step + 1)
                    if (debug) println(step + " rec res " + recRes)
                    if (recRes._1 == -1) {
                      return (-1, -1)
                    } else {
                      lowestBacktrackPoint = Math.min(recRes._1, lowestBacktrackPoint)
                    }
                  } else {
                    val nextTrans = sucCur.value().intIterator
                    var fastExit = false
                    while (nextTrans.hasNext() && !fastExit) {
                      val elem = nextTrans.next
                      var plannedBlockLevel = -1
                      val blockUpV = blockedForValue.get(toDoUp._2.id)
                      val blockUpT = blockedForTarget.get(toDoUp._2.id)
                      if (blockUpV != -1 || blockUpT != -1) {
                        val level = if (blockUpT != -1) { Math.max(blockUpT, toDoUp._1) } else { toDoUp._1 }
                        plannedBlockLevel = level
                        if (level != step) lockLevelsForTarget(step).add(level)
                        if (blockUpV != -1) {
                          if (blockUpV != step) lockLevelsForValue(step).add(blockUpV)
                          plannedBlockLevel = Math.max(plannedBlockLevel, blockUpV)
                        }
                      } else {
                        for (n <- toDoDown.tail) {
                          val blockDownV = blockedForValue.get(n._2.id)
                          val blockDownT = blockedForTarget.get(n._2.id)
                          if (blockDownV != -1 || blockDownT != -1) {
                            val level = if (blockDownT != -1) { Math.max(blockDownT, n._1) } else { n._1 }
                            if (plannedBlockLevel == -1 || level < plannedBlockLevel) {
                              plannedBlockLevel = level
                            }
                            if (level != step) lockLevelsForTarget(step).add(level)
                            if (blockDownV != -1) {
                              if (blockDownV != step) lockLevelsForValue(step).add(blockDownV)
                              plannedBlockLevel = Math.max(plannedBlockLevel, blockDownV)
                            }
                          }
                        }
                      }
                      if (plannedBlockLevel != -1) {
                        if (debug) println(step + " reseting locks (1) to " + plannedBlockLevel)
                        //                        if (plannedBlockLevel != step) {
                        lockLevelsForTarget(step).tailSet(plannedBlockLevel + 1).clear()
                        lockLevelsForValue(step).tailSet(plannedBlockLevel + 1).clear()
                        //                          lockLevelsForTarget(step).add(plannedBlockLevel)
                        //                        }
                        superFastExit = true
                        fastExit = true
                      } else {
                        if (debug) println(step + " trying " + nextStep + " by " + sucCur.key() + " with " + elem)
                        val filterTest = applyFilters(elem, filtersToApply)
                        if (filterTest == -1) {
                          val putRes = consumed.putIfAbsent(elem, step)
                          if (putRes == -1) {
                            pathByNodeId.put(nextStep.codeVertexId, elem)
                            val recRes = recExistsPath((step, sucCur.key()) :: toDoDown.tail, toDoUp, step + 1)
                            if (debug) println(step + " rec res " + recRes)
                            if (recRes._1 == -1) {
                              return (-1, -1)
                            } else {
                              lowestBacktrackPoint = Math.min(recRes._1, lowestBacktrackPoint)
                              //no need to check for target, we do it on next iteration of while anyway
                              if ((lockLevelsForValue(step + 1).isEmpty() || lockLevelsForValue(step + 1).last() < step)) fastExit = true
                              releaseForValue(step)
                            }
                            consumed.remove(elem)
                            pathByNodeId.remove(nextStep.codeVertexId)
                          } else {
                            lockLevelsForValue(step).add(putRes)
                          }
                        } else {
                          if (filterTest == step) lockLevelsForValue(step).add(0) else lockLevelsForTarget(step).add(filterTest)
                        }
                      }
                    }
                  }
                  removeFilters(sucCur.key().conditionalTransitions, step)
                } else {
                  if (debug) println("failed because of filter consistency")
                  lockLevelsForValue(step).add(filtersConsistency)
                }
              } else {
                if (debug) println("failed due to blocked status " + bsV + "-" + bsT)
                if (bsT != -1 && bsT != step) {
                  lockLevelsForTarget(step).add(bsT)
                }
                if (bsV != -1 && bsV != step) {
                  lockLevelsForValue(step).add(bsV)
                }
              }
            }
          } else {
            if (debug) println(step + " " + nextStep + " >1 successor")
            val nextTrans = nextStep.transitions.cursor()
            var fastExit = false
            while (nextTrans.moveNext() && !fastExit) {
              var plannedBlockLevel = -1
              val blockUpV = blockedForValue.get(toDoUp._2.id)
              val blockUpT = blockedForTarget.get(toDoUp._2.id)
              if (blockUpV != -1 || blockUpT != -1) {
                val level = if (blockUpT != -1) { Math.max(blockUpT, toDoUp._1) } else { toDoUp._1 }
                plannedBlockLevel = level
                if (level != step) lockLevelsForTarget(step).add(level)
                if (blockUpV != -1) {
                  if (blockUpV != step) lockLevelsForValue(step).add(blockUpV)
                  plannedBlockLevel = Math.max(plannedBlockLevel, blockUpV)
                }
              } else {
                for (n <- toDoDown.tail) {
                  val blockDownV = blockedForValue.get(n._2.id)
                  val blockDownT = blockedForTarget.get(n._2.id)
                  if (blockDownV != -1 || blockDownT != -1) {
                    val level = if (blockDownT != -1) { Math.max(blockDownT, n._1) } else { n._1 }
                    if (plannedBlockLevel == -1 || level < plannedBlockLevel) {
                      plannedBlockLevel = level
                    }
                    if (level != step) lockLevelsForTarget(step).add(level)
                    if (blockDownV != -1) {
                      if (blockDownV != step) lockLevelsForValue(step).add(blockDownV)
                      plannedBlockLevel = Math.max(plannedBlockLevel, blockDownV)
                    }
                  }
                }
              }
              if (plannedBlockLevel != -1) {
                if (debug) println(step + " reseting locks (2) to " + plannedBlockLevel)
                //                if (plannedBlockLevel != step) {
                lockLevelsForTarget(step).tailSet(plannedBlockLevel + 1).clear()
                lockLevelsForValue(step).tailSet(plannedBlockLevel + 1).clear()
                //                  lockLevelsForTarget(step).add(plannedBlockLevel)
                //                }
                fastExit = true
              } else {
                if (debug) println(step + " trying " + nextStep + " by " + nextTrans.value() + " with " + nextTrans.key())
                val putRes = consumed.putIfAbsent(nextTrans.key(), step)
                if (putRes == -1) {
                  pathByNodeId.put(nextStep.codeVertexId, nextTrans.key())
                  val additionalToDoDown = nextTrans.value().toList.map(p => (step, p))
                  var bsVal = -1
                  var bsTar = -1
                  for (n <- additionalToDoDown) {
                    val lT = blockedForTarget.get(n._2.id)
                    if (lT != -1) {
                      if (bsTar == -1 || lT < bsTar) {
                        bsTar = lT
                      }
                    }
                    val lV = blockedForValue.get(n._2.id)
                    if (lV != -1) {
                      if (bsVal == -1 || lV < bsVal) {
                        bsVal = lV
                      }
                    }
                  }
                  if (bsVal == -1 && bsTar == -1) {
                    var filtersConsistency = -1
                    for (n <- additionalToDoDown) {
                      val l = checkFiltersConsistency(n._2.conditionalTransitions)
                      if (l != -1 && (filtersConsistency == -1 || l < filtersConsistency)) {
                        filtersConsistency = l
                      }
                    }
                    if (filtersConsistency == -1) {
                      for (n <- additionalToDoDown) {
                        addFilters(n._2.conditionalTransitions, step, false)
                      }
                      val filtersToApply = filters.get(nextStep.codeVertexId)
                      val filterTest = applyFilters(nextTrans.key(), filtersToApply)
                      if (filterTest == -1) {
                        val recRes = recExistsPath(additionalToDoDown ++ toDoDown.tail, toDoUp, step + 1)
                        if (debug) println(step + " rec res " + recRes)
                        if (recRes._1 == -1) {
                          return (-1, -1)
                        } else {
                          lowestBacktrackPoint = Math.min(recRes._1, lowestBacktrackPoint)
                          releaseForValue(step)
                          releaseForTarget(step)
                        }
                      } else {
                        if (debug) println("failed because of filter test")
                        if (filterTest == step) lockLevelsForValue(step).add(0) else lockLevelsForTarget(step).add(filterTest)
                      }
                      for (n <- additionalToDoDown) {
                        removeFilters(n._2.conditionalTransitions, step)
                      }
                    } else {
                      if (debug) println("failed because of filter consistency")
                      lockLevelsForValue(step).add(filtersConsistency)
                    }
                  } else {
                    if (bsVal != -1 && bsVal != step) {
                      lockLevelsForValue(step).add(bsVal)
                    }
                    if (bsTar != -1 && bsTar != step) {
                      lockLevelsForTarget(step).add(bsTar)
                    }
                  }
                  consumed.remove(nextTrans.key())
                  pathByNodeId.remove(nextStep.codeVertexId)
                } else {
                  lockLevelsForValue(step).add(putRes)
                }
              }
            }
          }
        }
        if (locksGoingTo != step - 1) {
          if (debug) println(step + " transferring (value) " + lockLevelsForValue(step).tailSet(locksGoingTo).headSet(step - 1) + " to " + (step - 1))
          lockLevelsForValue(step - 1).addAll(lockLevelsForValue(step).tailSet(locksGoingTo).headSet(step - 1))
          if (debug) println(step + " transferring (target) " + locksGoingTo + " to " + (step - 1))
          lockLevelsForTarget(step - 1).add(locksGoingTo)
        }
        if (debug) println(step + " transferring (value) " + lockLevelsForValue(step).headSet(locksGoingTo) + " to " + locksGoingTo)
        lockLevelsForValue(locksGoingTo).addAll(lockLevelsForValue(step).headSet(locksGoingTo))
        if (debug) println(step + " transferring (target) " + lockLevelsForTarget(step).headSet(locksGoingTo) + " to " + locksGoingTo)
        lockLevelsForTarget(locksGoingTo).addAll(lockLevelsForTarget(step).headSet(locksGoingTo))
        if (lowestBacktrackPoint == step || lowestBacktrackPoint == locksGoingTo) {
          var locked = false
          if (!lockLevelsForValue(step).isEmpty()) {
            locked = true
            val lockingToVal = lockLevelsForValue(step).last()
            if (debug) println(step + " locking " + nextStep + " to " + lockingToVal + " (value)")
            if (lockingToVal >= step) throw new RuntimeException("not possible v " + step + "-" + lockLevelsForValue(step) + "-" + lockLevelsForTarget(step) + " " + this.id + " " + v)
            blockedForValue.put(nextStep.id, lockingToVal)
            releasePathsTransValueChange(lockingToVal).add(nextStep.id)
          }
          if (!lockLevelsForTarget(step).isEmpty()) {
            locked = true
            val lockingToTar = lockLevelsForTarget(step).last()
            if (debug) println(step + " locking " + nextStep + " to " + lockingToTar + " (target)")
            if (lockingToTar >= step) throw new RuntimeException("not possible t " + step + "-" + lockLevelsForValue(step) + "-" + lockLevelsForTarget(step) + " " + this.id + " " + v)
            blockedForTarget.put(nextStep.id, lockingToTar)
            releasePathsTransTargetChange(lockingToTar).add(nextStep.id)
          }
          if (!locked) {
            //lock forever
            if (debug) println(step + " locking " + nextStep + " forever")
            blockedForValue.put(nextStep.id, 0)
          }
          lowestBacktrackPoint = locksGoingTo
        }
        if (debug) println("end of exec for " + nextStep + " lowestBacktrackPoint " + lowestBacktrackPoint + " levels are target " + lockLevelsForTarget(step) + " value " + lockLevelsForValue(step))
        return (lowestBacktrackPoint, locksGoingTo)
      }

      var toDoUp = source
      var toDoDown: List[AutomatonNode] = if (outTrans != null) {
        outTrans.toList
      } else {
        Nil
      }
      if (checkFiltersConsistency(source.conditionalTransitions) != -1) return null
      addFilters(source.conditionalTransitions, 0, true)
      for (n <- toDoDown) {
        if (checkFiltersConsistency(n.conditionalTransitions) != -1) return null
        addFilters(n.conditionalTransitions, 0, false)
      }
      val res = recExistsPath(toDoDown.map(t => (0, t)), (0, toDoUp), 1)
      //      if (source.codeVertexId == 0 && debug && v == 2960 || source.codeVertexId == 1 && debug && v == 2840 || source.codeVertexId == 2 && debug && v == 3281 || source.codeVertexId == 3 && debug && v == 3306) {
      //        println("found: " + v + "->" + res._1 + " for id " + source.id)
      //      }
      if (res._1 == -1) {
        //        if (id == 6525 && v == 2215224) {
        //          for (i <- 0 until nbNodes) {
        //            print(pathByNodeId.get(i) + "\t")
        //          }
        //          println()
        //        }
        return consumed
      } else {
        return null
      }
    }
  }
}

object AutomaTreeNodeFilter {
  //  val mapsConfig = HashConfig.def
  val intintMapFacto: HashIntIntMapFactory = HashIntIntMaps.getDefaultFactory.withDefaultValue(-1)
  val DAG_CONSTRUCT_PAR = false

  //  println("hashmaps configuration: " + mapsConfig)
  def instantiateObjectObjectMap[K, V]: HashObjObjMap[K, V] = HashObjObjMaps.getDefaultFactory[K, V] /*.withHashConfig(mapsConfig)*/ .newMutableMap()
  //  def instantiateIntObjectMap[T >: Null]: IntObjMap[T] = new SortedListMap[T]
  //  def instantiateIntObjectMap[T >: Null](size: Int): IntObjMap[T] = new SortedListMap[T](size)
  def instantiateIntObjectMap[T]: HashIntObjMap[T] = HashIntObjMaps.getDefaultFactory[T] /*.withHashConfig(mapsConfig)*/ .newMutableMap()
  def instantiateIntObjectMap[T](size: Int): HashIntObjMap[T] = HashIntObjMaps.getDefaultFactory[T] /*.withHashConfig(mapsConfig)*/ .newMutableMap(size)
  def instantiateIntSet = HashIntSets.getDefaultFactory /*.withHashConfig(mapsConfig)*/ .newMutableSet()
  def instantiateIntSet(size: Int) = HashIntSets.getDefaultFactory /*.withHashConfig(mapsConfig)*/ .newMutableSet(size)
  def instantiateIntSet(content: IntSet) = HashIntSets.getDefaultFactory /*.withHashConfig(mapsConfig)*/ .newMutableSet(content)
  def instantiateIntIntMap(size: Int) = intintMapFacto /*.withHashConfig(mapsConfig)*/ .newMutableMap(size)
  def instantiateIntIntMap = intintMapFacto /*.withHashConfig(mapsConfig)*/ .newMutableMap

  def intersect(resAutomaton: AutomaTreeNodeFilter)(n1: AutomaTreeNodeFilter#AutomatonNode, n2: AutomaTreeNodeFilter#AutomatonNode, interType: IntersectType, threshold: Int, doubleFFsource: Int): resAutomaton.AutomatonNode = {
    if (!DAG_CONSTRUCT_PAR) {
      resAutomaton.transByLevel = Array.fill(resAutomaton.nbNodes)(AutomaTreeNodeFilter.instantiateIntSet)
      val revuz = Array.fill(resAutomaton.nbNodes)(new UnifiedMap[(IntObjMap[ArrayBuffer[resAutomaton.AutomatonNode]], List[(Int, fr.liglab.sami.utilities.Filter)]), resAutomaton.AutomatonNode])
      val r = intersectDfstrat(resAutomaton)(n1, n2, interType, HashLongObjMaps.newMutableMap(), null, doubleFFsource, revuz)
      if(r == null) return null
      resAutomaton.nodesByDepth(0) = IndexedSeq(r)
      import scala.collection.JavaConverters._
      for (i <- 1 until resAutomaton.nodesByDepth.size) {
        resAutomaton.nodesByDepth(i) = revuz(i).values().asScala.toIndexedSeq
      }
      resAutomaton.nodesByDepth.foreach(_.foreach(n => resAutomaton.transByLevel(n.codeVertexId).addAll(n.transitions.keySet)))
      for (l <- resAutomaton.transByLevel) {
        if (l.size() < threshold) {
          return null
        }
      }
      return r
    } else {
      val nodeBuffer = new ConcurrentHashMap[Long, resAutomaton.AutomatonNode]
      val revuz = Array.fill(resAutomaton.nbNodes)(new ConcurrentHashMap[(IntObjMap[ArrayBuffer[resAutomaton.AutomatonNode]], List[(Int, fr.liglab.sami.utilities.Filter)]), resAutomaton.AutomatonNode])
      resAutomaton.transByLevel = Array.fill(resAutomaton.nbNodes)(AutomaTreeNodeFilter.instantiateIntSet)
      //dummy value for the concurrenthashmap that does not accept null value
      resAutomaton.root = resAutomaton.newDummyNode
      val transitions = AutomaTreeNodeFilter.instantiateIntObjectMap[ArrayBuffer[resAutomaton.AutomatonNode]](n1.transitions.size())
      val keys = ArrayBuffer.empty[Int]
      val cur = n1.transitions.cursor()
      while (cur.moveNext) {
        keys += cur.key
      }
      keys.par.foreach(k => {
        val c2 = n2.transitions.get(k)
        if (c2 != null) {
          val c1 = n1.transitions.get(k)
          if (interType == IntersectType.FFtoFF && n1.codeVertexId == doubleFFsource) {
            var createdSuc: ArrayBuffer[resAutomaton.AutomatonNode] = ArrayBuffer.fill(c1.size + 1)(null)
            var allSucceeded = true
            var firstExtraFromCache = false
            var secondExtraFromCache = false
            var extraKey1 = 0L
            var extraKey2 = 0L
            for (i <- c1.size to 0 by -1 if allSucceeded) {
              if (i == c1.size) {
                extraKey1 = -(c2(i - 1).id.longValue())
                val extraTrans = {
                  val inB = nodeBuffer.get(extraKey1)
                  if (inB != null) {
                    firstExtraFromCache = true
                    inB
                  } else {
                    resAutomaton.newNode(c2(i - 1).codeVertexId + 1, i, c2(i - 1).transitions, null)
                  }
                }
                createdSuc(i) = extraTrans
              } else if (i == c1.size - 1) {
                extraKey2 = -((c1(i).id.longValue()) << 32)
                val extraTrans = {
                  val inB = nodeBuffer.get(extraKey2)
                  if (inB != null) {
                    secondExtraFromCache = true
                    inB
                  } else {
                    resAutomaton.newNode(c1(i).codeVertexId, i, c1(i).transitions, null)
                  }
                }
                createdSuc(i) = extraTrans
              } else {
                val childrenInter = intersectDfstratPar(resAutomaton)(c1(i), c2(i), interType, nodeBuffer, null, doubleFFsource, revuz)
                if (childrenInter != null) {
                  createdSuc(i) = childrenInter
                } else {
                  allSucceeded = false
                }
              }
            }
            if (allSucceeded) {
              if (!firstExtraFromCache) {
                val k = (createdSuc(createdSuc.size - 1).transitions, Nil)
                val existing = revuz(createdSuc(createdSuc.size - 1).codeVertexId).putIfAbsent(k, createdSuc(createdSuc.size - 1))
                if (existing == null) {
                } else {
                  createdSuc(createdSuc.size - 1) = existing
                }
                val bufPutRes = nodeBuffer.put(extraKey1, createdSuc(createdSuc.size - 1))
              }
              if (!secondExtraFromCache) {
                val k = (createdSuc(createdSuc.size - 2).transitions, Nil)
                val existing = revuz(createdSuc(createdSuc.size - 2).codeVertexId).putIfAbsent(k, createdSuc(createdSuc.size - 2))
                if (existing == null) {
                } else {
                  createdSuc(createdSuc.size - 2) = existing
                }
                nodeBuffer.put(extraKey2, createdSuc(createdSuc.size - 2))
              }
              transitions.synchronized(transitions.put(k, createdSuc))
            }
          } else if (c1 != null && c2 != null && c1.size == c2.size) {
            //standard case + BBtoBB + FFtoFB from same source
            var createdSuc: ArrayBuffer[resAutomaton.AutomatonNode] = null
            var allSucceeded = true
            for (i <- c1.size - 1 to 0 by -1 if allSucceeded) {
              val childrenInter = intersectDfstratPar(resAutomaton)(c1(i), c2(i), interType, nodeBuffer, null, doubleFFsource, revuz)
              if (childrenInter != null) {
                if (createdSuc == null) {
                  createdSuc = ArrayBuffer.fill(c1.size)(null)
                }
                createdSuc(i) = childrenInter
              } else {
                allSucceeded = false
              }
            }
            if (allSucceeded) {
              transitions.synchronized(transitions.put(k, createdSuc))
            }
          } else {
            interType match {
              case IntersectType.BBtoBB => {
                throw new RuntimeException("not good")
              }
              case IntersectType.FFtoFB => {
                var createdSuc: ArrayBuffer[resAutomaton.AutomatonNode] = null
                var allSucceeded = true
                for (i <- c1.size - 1 to 0 by -1 if allSucceeded) {
                  val childrenInter = intersectDfstratPar(resAutomaton)(c1(i), if (i == c1.size - 1) null else c2(i), interType, nodeBuffer, if (i == c1.size - 1) c2(c2.size - 1) else null, -1, revuz)
                  if (childrenInter != null) {
                    if (createdSuc == null) {
                      createdSuc = ArrayBuffer.fill(c1.size)(null)
                    }
                    createdSuc(i) = childrenInter
                  } else {
                    allSucceeded = false
                  }
                }
                if (allSucceeded) {
                  transitions.synchronized(transitions.put(k, createdSuc))
                }
              }
              case IntersectType.BFtoBF => {
                var createdSuc: ArrayBuffer[resAutomaton.AutomatonNode] = ArrayBuffer.fill(c2.size)(null)
                var allSucceeded = true
                var extraFromCache = false
                var extraKey = 0L
                for (i <- c2.size - 1 to 0 by -1 if allSucceeded) {
                  if (i == c2.size - 1) {
                    extraKey = -(c2(i).id.longValue())
                    val extraTrans = if (nodeBuffer.containsKey(extraKey)) {
                      extraFromCache = true
                      nodeBuffer.get(extraKey)
                    } else {
                      resAutomaton.newLeafNode(c2(i).codeVertexId, i, c2(i).transitions.keySet())
                    }
                    createdSuc(i) = extraTrans
                  } else {
                    val childrenInter = intersectDfstratPar(resAutomaton)(c1(i), c2(i), interType, nodeBuffer, null, doubleFFsource, revuz)
                    if (childrenInter != null) {
                      createdSuc(i) = childrenInter
                    } else {
                      allSucceeded = false
                    }
                  }
                }
                if (allSucceeded) {
                  if (!extraFromCache) {
                    val k = (createdSuc(createdSuc.size - 1).transitions, Nil)
                    val existing = revuz(createdSuc(createdSuc.size - 1).codeVertexId).putIfAbsent(k, createdSuc(createdSuc.size - 1))
                    if (existing == null) {
                    } else {
                      createdSuc(createdSuc.size - 1) = existing
                    }
                    nodeBuffer.put(extraKey, createdSuc(createdSuc.size - 1))
                  }
                  transitions.synchronized(transitions.put(k, createdSuc))
                }
              }
              case IntersectType.FFtoFF => {
                val maxSize = Math.max(if (c1 == null) 0 else c1.size, if (c2 == null) 0 else c2.size)
                val createdSuc: ArrayBuffer[resAutomaton.AutomatonNode] = ArrayBuffer.fill(maxSize)(null)
                var allSucceeded = true
                var extraFromCache = false
                var extraKey = 0L
                for (i <- maxSize - 1 to 0 by -1 if allSucceeded) {
                  if (i == maxSize - 1) {
                    extraKey = if (c1 == null || (c2 != null && c2.size > c1.size)) -(c2(i).id.longValue()) else -((c1(i).id.longValue()) << 32)
                    val extraTrans = if (nodeBuffer.containsKey(extraKey)) {
                      extraFromCache = true
                      nodeBuffer.get(extraKey)
                    } else {
                      if (c1 == null || (c2 != null && c2.size > c1.size)) {
                        resAutomaton.newNode(c2(i).codeVertexId + 1, i, c2(i).transitions, null)
                      } else {
                        resAutomaton.newNode(c1(i).codeVertexId, i, c1(i).transitions, null)
                      }
                    }
                    createdSuc(i) = extraTrans
                  } else {
                    val childrenInter = intersectDfstratPar(resAutomaton)(c1(i), c2(i), interType, nodeBuffer, null, doubleFFsource, revuz)
                    if (childrenInter != null) {
                      createdSuc(i) = childrenInter
                    } else {
                      allSucceeded = false
                    }
                  }
                }
                if (allSucceeded) {
                  if (!extraFromCache) {
                    val k = (createdSuc(createdSuc.size - 1).transitions, Nil)
                    val existing = revuz(createdSuc(createdSuc.size - 1).codeVertexId).putIfAbsent(k, createdSuc(createdSuc.size - 1))
                    if (existing == null) {
                    } else {
                      createdSuc(createdSuc.size - 1) = existing
                    }
                    nodeBuffer.put(extraKey, createdSuc(createdSuc.size - 1))
                  }
                  transitions.synchronized(transitions.put(k, createdSuc))
                }
              }
            }
          }
        }
      })
      if (transitions.isEmpty()) return null
      val r = resAutomaton.newNode(n1.codeVertexId, n1.childPos, transitions)
      resAutomaton.nodesByDepth(0) = IndexedSeq(r)
      import scala.collection.JavaConverters._
      for (i <- 1 until resAutomaton.nodesByDepth.size) {
        resAutomaton.nodesByDepth(i) = revuz(i).values().asScala.toIndexedSeq
      }
      resAutomaton.nodesByDepth.par.foreach(_.foreach(n => resAutomaton.transByLevel(n.codeVertexId).addAll(n.transitions.keySet)))
      for (l <- resAutomaton.transByLevel) {
        if (l.size() < threshold) {
          return null
        }
      }
      for (l <- resAutomaton.transByLevel) {
        if (l.size() < threshold) {
          return null
        }
      }
      return r
    }
  }

  def extend(resAutomaton: AutomaTreeNodeFilter)(n: AutomaTreeNodeFilter#AutomatonNode, extensions: IntObjMap[_ <: ArrayBuffer[_ <: AutomaTreeNodeFilter#AutomatonNode]], threshold: Int): resAutomaton.AutomatonNode = {
    if (!DAG_CONSTRUCT_PAR) {
      resAutomaton.transByLevel = Array.fill(resAutomaton.nbNodes)(AutomaTreeNodeFilter.instantiateIntSet)
      val d1Buff: IntObjMap[resAutomaton.AutomatonNode] = AutomaTreeNodeFilter.instantiateIntObjectMap
      val leavesBuff: IntObjMap[resAutomaton.AutomatonNode] = AutomaTreeNodeFilter.instantiateIntObjectMap
      val revuz = Array.fill(resAutomaton.nbNodes)(new UnifiedMap[(IntObjMap[ArrayBuffer[resAutomaton.AutomatonNode]], List[(Int, fr.liglab.sami.utilities.Filter)]), resAutomaton.AutomatonNode])
      val r = extendImp(resAutomaton)(n, extensions, d1Buff, leavesBuff, revuz)
      if(r == null) return null
      resAutomaton.nodesByDepth(0) = IndexedSeq(r)
      import scala.collection.JavaConverters._
      for (i <- 1 until resAutomaton.nodesByDepth.size - 1) {
        resAutomaton.nodesByDepth(i) = revuz(i).values().asScala.toIndexedSeq
      }
      resAutomaton.nodesByDepth(resAutomaton.nbNodes - 1) = leavesBuff.values().asScala.toIndexedSeq
      resAutomaton.nodesByDepth.par.foreach(_.foreach(n => resAutomaton.transByLevel(n.codeVertexId).addAll(n.transitions.keySet)))
      for (l <- resAutomaton.transByLevel) {
        if (l.size() < threshold) {
          return null
        }
      }
      return r
    } else {
      resAutomaton.transByLevel = Array.fill(resAutomaton.nbNodes)(AutomaTreeNodeFilter.instantiateIntSet)
      resAutomaton.root = resAutomaton.newDummyNode
      val d1Buff: ConcurrentHashMap[Integer, resAutomaton.AutomatonNode] = new ConcurrentHashMap
      val leavesBuff: ConcurrentHashMap[Integer, resAutomaton.AutomatonNode] = new ConcurrentHashMap
      val revuz = Array.fill(resAutomaton.nbNodes)(new ConcurrentHashMap[(IntObjMap[ArrayBuffer[resAutomaton.AutomatonNode]], List[(Int, fr.liglab.sami.utilities.Filter)]), resAutomaton.AutomatonNode])
      val transitions = AutomaTreeNodeFilter.instantiateIntObjectMap[ArrayBuffer[resAutomaton.AutomatonNode]](n.transitions.size())
      val keys = ArrayBuffer.empty[Int]
      val cur = n.transitions.cursor()
      while (cur.moveNext) {
        keys += cur.key
      }
      keys.par.foreach(k => {
        val successors = n.transitions.get(k)
        var createdSuc: ArrayBuffer[resAutomaton.AutomatonNode] = null
        var allSucceeded = true
        for (i <- 0 until successors.size if allSucceeded) {
          val rec = extendImpPar(resAutomaton)(successors(i), extensions, d1Buff, leavesBuff, revuz)
          if (rec == null) {
            allSucceeded = false
          } else {
            if (createdSuc == null) createdSuc = new ArrayBuffer(successors.size)
            createdSuc += rec
          }
        }
        if (allSucceeded) {
          transitions.synchronized(transitions.put(k, createdSuc))
        }
      })
      if (transitions.isEmpty()) return null
      val r = resAutomaton.newNode(n.codeVertexId, n.childPos, transitions)
      resAutomaton.nodesByDepth(0) = IndexedSeq(r)
      import scala.collection.JavaConverters._
      for (i <- 1 until resAutomaton.nodesByDepth.size - 1) {
        resAutomaton.nodesByDepth(i) = revuz(i).values().asScala.toIndexedSeq
      }
      resAutomaton.nodesByDepth(resAutomaton.nbNodes - 1) = leavesBuff.values().asScala.toIndexedSeq
      resAutomaton.nodesByDepth.par.foreach(_.foreach(n => resAutomaton.transByLevel(n.codeVertexId).addAll(n.transitions.keySet)))
      for (l <- resAutomaton.transByLevel) {
        if (l.size() < threshold) {
          return null
        }
      }
      r.conditionalTransitions = n.conditionalTransitions
      r.backEdgeSource = n.backEdgeSource
      return r
    }
  }

  def project(resAutomaton: AutomaTreeNodeFilter)(n: AutomaTreeNodeFilter#AutomatonNode, filter: Array[IntSet], threshold: Int): resAutomaton.AutomatonNode = {
    if (!DAG_CONSTRUCT_PAR) {
      val d1Buff: IntObjMap[resAutomaton.AutomatonNode] = AutomaTreeNodeFilter.instantiateIntObjectMap
      val transByLevel: Array[IntSet] = Array.fill(resAutomaton.nbNodes)(AutomaTreeNodeFilter.instantiateIntSet)
      val revuz = Array.fill(resAutomaton.nbNodes)(new UnifiedMap[(IntObjMap[ArrayBuffer[resAutomaton.AutomatonNode]], List[(Int, fr.liglab.sami.utilities.Filter)]), resAutomaton.AutomatonNode])
      val r = projectImp(resAutomaton)(n, filter, d1Buff, revuz)
      if(r == null) return null
      import scala.collection.JavaConverters._
      transByLevel(0).addAll(r.transitions.keySet)
      revuz.par.foreach(r => r.values().asScala.foreach(n => transByLevel(n.codeVertexId).addAll(n.transitions.keySet)))
      for (l <- transByLevel) {
        if (l.size() < threshold) {
          return null
        }
      }
      r.conditionalTransitions = n.conditionalTransitions
      r.backEdgeSource = n.backEdgeSource
      return r
    } else {
      val d1Buff: ConcurrentHashMap[Integer, resAutomaton.AutomatonNode] = new ConcurrentHashMap
      resAutomaton.root = resAutomaton.newDummyNode
      val transByLevel: Array[IntSet] = Array.fill(resAutomaton.nbNodes)(AutomaTreeNodeFilter.instantiateIntSet)
      val revuz = Array.fill(resAutomaton.nbNodes)(new ConcurrentHashMap[(IntObjMap[ArrayBuffer[resAutomaton.AutomatonNode]], List[(Int, fr.liglab.sami.utilities.Filter)]), resAutomaton.AutomatonNode])
      val transitions = AutomaTreeNodeFilter.instantiateIntObjectMap[ArrayBuffer[resAutomaton.AutomatonNode]](n.transitions.size())
      val keys = ArrayBuffer.empty[Int]
      val cur = n.transitions.cursor()
      while (cur.moveNext) {
        keys += cur.key
      }
      keys.par.foreach(k => {
        if (filter(0).contains(k)) {
          val successors = n.transitions.get(k)
          if (successors != null) {
            var createdSuc: ArrayBuffer[resAutomaton.AutomatonNode] = null
            var allSucceeded = true
            for (i <- 0 until successors.size if allSucceeded) {
              val rec = projectImpPar(resAutomaton)(successors(i), filter, d1Buff, revuz)
              if (rec == null) {
                allSucceeded = false
              } else {
                if (createdSuc == null) createdSuc = new ArrayBuffer(successors.size)
                createdSuc += rec
              }
            }
            if (allSucceeded) {
              transitions.synchronized(transitions.put(k, createdSuc))
            }
          } else {
            transitions.synchronized(transitions.put(k, null))
          }
        }
      })
      if (transitions.isEmpty()) return null
      val r = resAutomaton.newNode(n.codeVertexId, n.childPos, transitions)
      transByLevel(0).addAll(r.transitions.keySet)
      import scala.collection.JavaConverters._
      revuz.par.foreach(r => r.values().asScala.foreach(n => transByLevel(n.codeVertexId).addAll(n.transitions.keySet)))
      for (l <- transByLevel) {
        if (l.size() < threshold) {
          return null
        }
      }
      r.conditionalTransitions = n.conditionalTransitions
      r.backEdgeSource = n.backEdgeSource
      return r
    }
  }

  private def mergeCondTrans(l1: List[(Int, fr.liglab.sami.utilities.Filter)], l2: List[(Int, fr.liglab.sami.utilities.Filter)]): List[(Int, fr.liglab.sami.utilities.Filter)] = {
    if (l1.isEmpty) return l2
    if (l2.isEmpty) return l1
    val grouped = (l1 ::: l2).groupBy(_._1)
    var res: List[(Int, fr.liglab.sami.utilities.Filter)] = Nil
    for (g <- grouped) {
      if (g._2.tail.isEmpty) {
        res = g._2.head :: res
      } else {
        val s1 = g._2.head._2.asInstanceOf[ExactFilter]
        val s2 = g._2.tail.head._2.asInstanceOf[ExactFilter]
        if (s1.f.size > s2.f.size) {
          if (s1.f.containsAll(s2.f)) {
            res = g._2.tail.head :: res
          } else {
            val copy = HashIntSets.newMutableSet(s2.f)
            copy.retainAll(s1.f)
            if (copy.isEmpty()) { return null } else { res = (g._1, new ExactFilter(copy)) :: res }
          }
        } else {
          if (s2.f.containsAll(s1.f)) {
            res = g._2.head :: res
          } else {
            val copy = HashIntSets.newMutableSet(s1.f)
            copy.retainAll(s2.f)
            if (copy.isEmpty()) { return null } else { res = (g._1, new ExactFilter(copy)) :: res }
          }
        }
      }
    }
    return res
  }

  private def addCondTrans(l: List[(Int, fr.liglab.sami.utilities.Filter)], f: fr.liglab.sami.utilities.Filter, p: Int): List[(Int, fr.liglab.sami.utilities.Filter)] = {
    var found = false;
    val updated = l.map {
      e =>
        if (e._1 == p) {
          found = true
          val s1 = e._2.asInstanceOf[ExactFilter]
          val s2 = f.asInstanceOf[ExactFilter]
          if (s1.f.size > s2.f.size) {
            if (s1.f.containsAll(s2.f)) {
              (p, f)
            } else {
              val copy = HashIntSets.newMutableSet(s2.f)
              copy.retainAll(s1.f)
              if (copy.isEmpty()) { return null } else { (p, new ExactFilter(copy)) }
            }
          } else {
            if (s2.f.containsAll(s1.f)) {
              e
            } else {
              val copy = HashIntSets.newMutableSet(s1.f)
              copy.retainAll(s2.f)
              if (copy.isEmpty()) { return null } else { (p, new ExactFilter(copy)) }
            }
          }
        } else {
          e
        }
    }
    if (found) return updated
    return (p, f) :: updated
  }

  private def intersectDfstrat(resAutomaton: AutomaTreeNodeFilter)(n1: AutomaTreeNodeFilter#AutomatonNode, n2: AutomaTreeNodeFilter#AutomatonNode, interType: IntersectType, nodeBuffer: LongObjMap[resAutomaton.AutomatonNode], extraTransitionsFromN2: AutomaTreeNodeFilter#AutomatonNode, doubleFFsource: Int, revuz: Array[UnifiedMap[(IntObjMap[ArrayBuffer[resAutomaton.AutomatonNode]], List[(Int, fr.liglab.sami.utilities.Filter)]), resAutomaton.AutomatonNode]]): resAutomaton.AutomatonNode = {
    val key = if (extraTransitionsFromN2 == null && n2 == null) {
      (n1.id.longValue() << 32)
    } else if (extraTransitionsFromN2 == null) {
      (n1.id.longValue() << 32) | (n2.id.longValue() & 0xffffffffL)
    } else {
      (n1.id.longValue() << 32) | (extraTransitionsFromN2.id.longValue() & 0xffffffffL)
    }
    if (key < 0L) throw new RuntimeException("should be pos")
    if (nodeBuffer.containsKey(key)) {
      return nodeBuffer.get(key)
    }
    val filter = if (extraTransitionsFromN2 == null && n2 == null) {
      n1.conditionalTransitions
    } else if (extraTransitionsFromN2 == null) {
      mergeCondTrans(n1.conditionalTransitions, n2.conditionalTransitions)
    } else {
      addCondTrans(n1.conditionalTransitions, new ExactFilter(extraTransitionsFromN2.transitions.keySet()), extraTransitionsFromN2.codeVertexId)
    }
    var transitions: IntObjMap[ArrayBuffer[resAutomaton.AutomatonNode]] = null
    if (filter != null) {
      if (n2 == null) {
        val cursor = n1.transitions.cursor()
        while (cursor.moveNext()) {
          if (cursor.value() == null) {
            if (transitions == null) {
              transitions = new FakeMap(AutomaTreeNodeFilter.instantiateIntSet, null)
            }
            transitions.put(cursor.key(), null)
          } else {
            val c1 = cursor.value()
            var createdSuc: ArrayBuffer[resAutomaton.AutomatonNode] = null
            var allSucceeded = true
            for (i <- c1.size - 1 to 0 by -1 if allSucceeded) {
              val childrenInter = intersectDfstrat(resAutomaton)(c1(i), null, interType, nodeBuffer, null, doubleFFsource, revuz)
              if (childrenInter != null) {
                if (createdSuc == null) {
                  createdSuc = ArrayBuffer.fill(c1.size)(null)
                }
                createdSuc(i) = childrenInter
              } else {
                allSucceeded = false
              }
            }
            if (allSucceeded) {
              if (transitions == null) {
                transitions = AutomaTreeNodeFilter.instantiateIntObjectMap[ArrayBuffer[resAutomaton.AutomatonNode]]
              }
              transitions.put(cursor.key(), createdSuc)
            }
          }
        }
      } else {
        val (small, large, swapped) = if (n1.transitions.size() < n2.transitions.size()) {
          (n1, n2, false)
        } else {
          (n2, n1, true)
        }
        val cursor = small.transitions.cursor()
        while (cursor.moveNext()) {
          if (large.transitions.containsKey(cursor.key)) {
            val cl = large.transitions.get(cursor.key())
            val cs = cursor.value()
            val (c1, c2) = if (swapped) {
              (cl, cs)
            } else {
              (cs, cl)
            }
            if (c1 == null && c2 == null) {
              if (transitions == null) {
                transitions = new FakeMap(AutomaTreeNodeFilter.instantiateIntSet, null)
              }
              transitions.put(cursor.key(), null)
            } else if (interType == IntersectType.FFtoFF && n1.codeVertexId == doubleFFsource) {
              var createdSuc: ArrayBuffer[resAutomaton.AutomatonNode] = ArrayBuffer.fill(c1.size + 1)(null)
              var allSucceeded = true
              var firstExtraFromCache = false
              var secondExtraFromCache = false
              var extraKey1 = 0L
              var extraKey2 = 0L
              for (i <- c1.size to 0 by -1 if allSucceeded) {
                if (i == c1.size) {
                  extraKey1 = -(c2(i - 1).id.longValue())
                  val extraTrans = if (nodeBuffer.containsKey(extraKey1)) {
                    firstExtraFromCache = true
                    nodeBuffer.get(extraKey1)
                  } else {
                    resAutomaton.newNode(c2(i - 1).codeVertexId + 1, i, c2(i - 1).transitions, null)
                  }
                  createdSuc(i) = extraTrans
                } else if (i == c1.size - 1) {
                  extraKey2 = -((c1(i).id.longValue()) << 32)
                  val extraTrans = if (nodeBuffer.containsKey(extraKey2)) {
                    secondExtraFromCache = true
                    nodeBuffer.get(extraKey2)
                  } else {
                    resAutomaton.newNode(c1(i).codeVertexId, i, c1(i).transitions, null)
                  }
                  createdSuc(i) = extraTrans
                } else {
                  val childrenInter = intersectDfstrat(resAutomaton)(c1(i), c2(i), interType, nodeBuffer, null, doubleFFsource, revuz)
                  if (childrenInter != null) {
                    createdSuc(i) = childrenInter
                  } else {
                    allSucceeded = false
                  }
                }
              }
              if (allSucceeded) {
                if (transitions == null) {
                  transitions = AutomaTreeNodeFilter.instantiateIntObjectMap[ArrayBuffer[resAutomaton.AutomatonNode]]
                }
                if (!firstExtraFromCache) {
                  //TODO revuz shouldn t be necessary here if we trust that there are no dups in initial dags
                  val k = (createdSuc(createdSuc.size - 1).transitions, Nil)
                  val existing = revuz(createdSuc(createdSuc.size - 1).codeVertexId).get(k)
                  if (existing == null) {
                    revuz(createdSuc(createdSuc.size - 1).codeVertexId).put(k, createdSuc(createdSuc.size - 1))
                  } else {
                    createdSuc(createdSuc.size - 1) = existing
                  }
                  nodeBuffer.put(extraKey1, createdSuc(createdSuc.size - 1))
                }
                if (!secondExtraFromCache) {
                  //TODO revuz shouldn t be necessary here if we trust that there are no dups in initial dags
                  val k = (createdSuc(createdSuc.size - 2).transitions, Nil)
                  val existing = revuz(createdSuc(createdSuc.size - 2).codeVertexId).get(k)
                  if (existing == null) {
                    revuz(createdSuc(createdSuc.size - 2).codeVertexId).put(k, createdSuc(createdSuc.size - 2))
                  } else {
                    createdSuc(createdSuc.size - 2) = existing
                  }
                  nodeBuffer.put(extraKey2, createdSuc(createdSuc.size - 2))
                }
                transitions.put(cursor.key(), createdSuc)
              }
            } else if (c1 != null && c2 != null && c1.size == c2.size) {
              //standard case + BBtoBB + FFtoFB from same source
              var createdSuc: ArrayBuffer[resAutomaton.AutomatonNode] = null
              var allSucceeded = true
              for (i <- c1.size - 1 to 0 by -1 if allSucceeded) {
                val childrenInter = intersectDfstrat(resAutomaton)(c1(i), c2(i), interType, nodeBuffer, null, doubleFFsource, revuz)
                if (childrenInter != null) {
                  if (createdSuc == null) {
                    createdSuc = ArrayBuffer.fill(c1.size)(null)
                  }
                  createdSuc(i) = childrenInter
                } else {
                  allSucceeded = false
                }
              }
              if (allSucceeded) {
                if (transitions == null) {
                  transitions = AutomaTreeNodeFilter.instantiateIntObjectMap[ArrayBuffer[resAutomaton.AutomatonNode]]
                }
                transitions.put(cursor.key(), createdSuc)
              }
            } else {
              interType match {
                case IntersectType.BBtoBB => {
                  throw new RuntimeException("not good")
                }
                case IntersectType.FFtoFB => {
                  var createdSuc: ArrayBuffer[resAutomaton.AutomatonNode] = null
                  var allSucceeded = true
                  for (i <- c1.size - 1 to 0 by -1 if allSucceeded) {
                    val childrenInter = intersectDfstrat(resAutomaton)(c1(i), if (i == c1.size - 1) null else c2(i), interType, nodeBuffer, if (i == c1.size - 1) c2(c2.size - 1) else null, -1, revuz)
                    if (childrenInter != null) {
                      if (createdSuc == null) {
                        createdSuc = ArrayBuffer.fill(c1.size)(null)
                      }
                      createdSuc(i) = childrenInter
                    } else {
                      allSucceeded = false
                    }
                  }
                  if (allSucceeded) {
                    if (transitions == null) {
                      transitions = AutomaTreeNodeFilter.instantiateIntObjectMap[ArrayBuffer[resAutomaton.AutomatonNode]]
                    }
                    transitions.put(cursor.key(), createdSuc)
                  }
                }
                case IntersectType.BFtoBF => {
                  var createdSuc: ArrayBuffer[resAutomaton.AutomatonNode] = ArrayBuffer.fill(c2.size)(null)
                  var allSucceeded = true
                  var extraFromCache = false
                  var extraKey = 0L
                  for (i <- c2.size - 1 to 0 by -1 if allSucceeded) {
                    if (i == c2.size - 1) {
                      extraKey = -(c2(i).id.longValue())
                      val extraTrans = if (nodeBuffer.containsKey(extraKey)) {
                        extraFromCache = true
                        nodeBuffer.get(extraKey)
                      } else {
                        resAutomaton.newLeafNode(c2(i).codeVertexId, i, c2(i).transitions.keySet())
                      }
                      createdSuc(i) = extraTrans
                    } else {
                      val childrenInter = intersectDfstrat(resAutomaton)(c1(i), c2(i), interType, nodeBuffer, null, doubleFFsource, revuz)
                      if (childrenInter != null) {
                        createdSuc(i) = childrenInter
                      } else {
                        allSucceeded = false
                      }
                    }
                  }
                  if (allSucceeded) {
                    if (!extraFromCache) {
                      val k = (createdSuc(createdSuc.size - 1).transitions, Nil)

                      val existing = revuz(createdSuc(createdSuc.size - 1).codeVertexId).get(k)
                      if (existing == null) {
                        revuz(createdSuc(createdSuc.size - 1).codeVertexId).put(k, createdSuc(createdSuc.size - 1))
                      } else {
                        createdSuc(createdSuc.size - 1) = existing
                      }
                      nodeBuffer.put(extraKey, createdSuc(createdSuc.size - 1))
                    }
                    if (transitions == null) {
                      transitions = AutomaTreeNodeFilter.instantiateIntObjectMap[ArrayBuffer[resAutomaton.AutomatonNode]]
                    }
                    transitions.put(cursor.key(), createdSuc)
                  }
                }
                case IntersectType.FFtoFF => {
                  val maxSize = Math.max(if (c1 == null) 0 else c1.size, if (c2 == null) 0 else c2.size)
                  val createdSuc: ArrayBuffer[resAutomaton.AutomatonNode] = ArrayBuffer.fill(maxSize)(null)
                  var allSucceeded = true
                  var extraFromCache = false
                  var extraKey = 0L
                  for (i <- maxSize - 1 to 0 by -1 if allSucceeded) {
                    if (i == maxSize - 1) {
                      extraKey = if (c1 == null || (c2 != null && c2.size > c1.size)) -(c2(i).id.longValue()) else -((c1(i).id.longValue()) << 32)
                      val extraTrans = if (nodeBuffer.containsKey(extraKey)) {
                        extraFromCache = true
                        nodeBuffer.get(extraKey)
                      } else {
                        if (c1 == null || (c2 != null && c2.size > c1.size)) {
                          resAutomaton.newNode(c2(i).codeVertexId + 1, i, c2(i).transitions, null)
                        } else {
                          resAutomaton.newNode(c1(i).codeVertexId, i, c1(i).transitions, null)
                        }
                      }
                      createdSuc(i) = extraTrans
                    } else {
                      val childrenInter = intersectDfstrat(resAutomaton)(c1(i), c2(i), interType, nodeBuffer, null, doubleFFsource, revuz)
                      if (childrenInter != null) {
                        createdSuc(i) = childrenInter
                      } else {
                        allSucceeded = false
                      }
                    }
                  }
                  if (allSucceeded) {
                    if (!extraFromCache) {
                      val k = (createdSuc(createdSuc.size - 1).transitions, Nil)
                      val existing = revuz(createdSuc(createdSuc.size - 1).codeVertexId).get(k)
                      if (existing == null) {
                        revuz(createdSuc(createdSuc.size - 1).codeVertexId).put(k, createdSuc(createdSuc.size - 1))
                      } else {
                        createdSuc(createdSuc.size - 1) = existing
                      }
                      nodeBuffer.put(extraKey, createdSuc(createdSuc.size - 1))
                    }
                    if (transitions == null) {
                      transitions = AutomaTreeNodeFilter.instantiateIntObjectMap[ArrayBuffer[resAutomaton.AutomatonNode]]
                    }
                    transitions.put(cursor.key(), createdSuc)
                  }
                }
              }
            }
          }
        }
      }
    }
    if (transitions == null) {
      nodeBuffer.put(key, null)
      return null
    } else {
      val k = (transitions, filter)
      val existing = revuz(n1.codeVertexId).get(k)
      val intersectionRes = if (existing == null) {
        val n = resAutomaton.newNode(n1.codeVertexId, n1.childPos, transitions)
        n.backEdgeSource = n1.backEdgeSource || (interType == IntersectType.FFtoFB && n.codeVertexId == resAutomaton.nbNodes - 1)
        n.conditionalTransitions = filter
        revuz(n.codeVertexId).put(k, n)
        n
      } else {
        existing
      }
      nodeBuffer.put(key, intersectionRes)
      return intersectionRes
    }
  }

  private def intersectDfstratPar(resAutomaton: AutomaTreeNodeFilter)(n1: AutomaTreeNodeFilter#AutomatonNode, n2: AutomaTreeNodeFilter#AutomatonNode, interType: IntersectType, nodeBuffer: ConcurrentHashMap[Long, resAutomaton.AutomatonNode], extraTransitionsFromN2: AutomaTreeNodeFilter#AutomatonNode, doubleFFsource: Int, revuz: Array[ConcurrentHashMap[(IntObjMap[ArrayBuffer[resAutomaton.AutomatonNode]], List[(Int, fr.liglab.sami.utilities.Filter)]), resAutomaton.AutomatonNode]]): resAutomaton.AutomatonNode = {
    val key = if (extraTransitionsFromN2 == null && n2 == null) {
      (n1.id.longValue() << 32)
    } else if (extraTransitionsFromN2 == null) {
      (n1.id.longValue() << 32) | (n2.id.longValue() & 0xffffffffL)
    } else {
      (n1.id.longValue() << 32) | (extraTransitionsFromN2.id.longValue() & 0xffffffffL)
    }
    if (key < 0L) throw new RuntimeException("should be pos")
    val inNodeBuffer = nodeBuffer.get(key)
    if (inNodeBuffer == resAutomaton.root) return null else if (inNodeBuffer != null) return inNodeBuffer
    val filter = if (extraTransitionsFromN2 == null && n2 == null) {
      n1.conditionalTransitions
    } else if (extraTransitionsFromN2 == null) {
      mergeCondTrans(n1.conditionalTransitions, n2.conditionalTransitions)
    } else {
      addCondTrans(n1.conditionalTransitions, new ExactFilter(extraTransitionsFromN2.transitions.keySet()), extraTransitionsFromN2.codeVertexId)
    }
    var transitions: IntObjMap[ArrayBuffer[resAutomaton.AutomatonNode]] = null
    if (filter != null) {
      if (n2 == null) {
        val cursor = n1.transitions.cursor()
        while (cursor.moveNext()) {
          if (cursor.value() == null) {
            if (transitions == null) {
              transitions = new FakeMap(AutomaTreeNodeFilter.instantiateIntSet, null)
            }
            transitions.put(cursor.key(), null)
          } else {
            val c1 = cursor.value()
            var createdSuc: ArrayBuffer[resAutomaton.AutomatonNode] = null
            var allSucceeded = true
            for (i <- c1.size - 1 to 0 by -1 if allSucceeded) {
              val childrenInter = intersectDfstratPar(resAutomaton)(c1(i), null, interType, nodeBuffer, null, doubleFFsource, revuz)
              if (childrenInter != null) {
                if (createdSuc == null) {
                  createdSuc = ArrayBuffer.fill(c1.size)(null)
                }
                createdSuc(i) = childrenInter
              } else {
                allSucceeded = false
              }
            }
            if (allSucceeded) {
              if (transitions == null) {
                transitions = AutomaTreeNodeFilter.instantiateIntObjectMap[ArrayBuffer[resAutomaton.AutomatonNode]]
              }
              transitions.put(cursor.key(), createdSuc)
            }
          }
        }
      } else {
        val (small, large, swapped) = if (n1.transitions.size() < n2.transitions.size()) {
          (n1, n2, false)
        } else {
          (n2, n1, true)
        }
        val cursor = small.transitions.cursor()
        while (cursor.moveNext()) {
          if (large.transitions.containsKey(cursor.key)) {
            val cl = large.transitions.get(cursor.key())
            val cs = cursor.value()
            val (c1, c2) = if (swapped) {
              (cl, cs)
            } else {
              (cs, cl)
            }
            if (c1 == null && c2 == null) {
              if (transitions == null) {
                transitions = new FakeMap(AutomaTreeNodeFilter.instantiateIntSet, null)
              }
              transitions.put(cursor.key(), null)
            } else if (interType == IntersectType.FFtoFF && n1.codeVertexId == doubleFFsource) {
              var createdSuc: ArrayBuffer[resAutomaton.AutomatonNode] = ArrayBuffer.fill(c1.size + 1)(null)
              var allSucceeded = true
              var firstExtraFromCache = false
              var secondExtraFromCache = false
              var extraKey1 = 0L
              var extraKey2 = 0L
              for (i <- c1.size to 0 by -1 if allSucceeded) {
                if (i == c1.size) {
                  extraKey1 = -(c2(i - 1).id.longValue())
                  val extraTrans = if (nodeBuffer.containsKey(extraKey1)) {
                    firstExtraFromCache = true
                    nodeBuffer.get(extraKey1)
                  } else {
                    resAutomaton.newNode(c2(i - 1).codeVertexId + 1, i, c2(i - 1).transitions, null)
                  }
                  createdSuc(i) = extraTrans
                } else if (i == c1.size - 1) {
                  extraKey2 = -((c1(i).id.longValue()) << 32)
                  val extraTrans = if (nodeBuffer.containsKey(extraKey2)) {
                    secondExtraFromCache = true
                    nodeBuffer.get(extraKey2)
                  } else {
                    resAutomaton.newNode(c1(i).codeVertexId, i, c1(i).transitions, null)
                  }
                  createdSuc(i) = extraTrans
                } else {
                  val childrenInter = intersectDfstratPar(resAutomaton)(c1(i), c2(i), interType, nodeBuffer, null, doubleFFsource, revuz)
                  if (childrenInter != null) {
                    createdSuc(i) = childrenInter
                  } else {
                    allSucceeded = false
                  }
                }
              }
              if (allSucceeded) {
                if (transitions == null) {
                  transitions = AutomaTreeNodeFilter.instantiateIntObjectMap[ArrayBuffer[resAutomaton.AutomatonNode]]
                }
                if (!firstExtraFromCache) {
                  val k = (createdSuc(createdSuc.size - 1).transitions, Nil)
                  val existing = revuz(createdSuc(createdSuc.size - 1).codeVertexId).putIfAbsent(k, createdSuc(createdSuc.size - 1))
                  if (existing == null) {
                  } else {
                    createdSuc(createdSuc.size - 1) = existing
                  }
                  val bufPutRes = nodeBuffer.put(extraKey1, createdSuc(createdSuc.size - 1))
                }
                if (!secondExtraFromCache) {
                  val k = (createdSuc(createdSuc.size - 2).transitions, Nil)
                  val existing = revuz(createdSuc(createdSuc.size - 2).codeVertexId).putIfAbsent(k, createdSuc(createdSuc.size - 2))
                  if (existing == null) {
                  } else {
                    createdSuc(createdSuc.size - 2) = existing
                  }
                  nodeBuffer.put(extraKey2, createdSuc(createdSuc.size - 2))
                }
                transitions.put(cursor.key(), createdSuc)
              }
            } else if (c1 != null && c2 != null && c1.size == c2.size) {
              //standard case + BBtoBB + FFtoFB from same source
              var createdSuc: ArrayBuffer[resAutomaton.AutomatonNode] = null
              var allSucceeded = true
              for (i <- c1.size - 1 to 0 by -1 if allSucceeded) {
                val childrenInter = intersectDfstratPar(resAutomaton)(c1(i), c2(i), interType, nodeBuffer, null, doubleFFsource, revuz)
                if (childrenInter != null) {
                  if (createdSuc == null) {
                    createdSuc = ArrayBuffer.fill(c1.size)(null)
                  }
                  createdSuc(i) = childrenInter
                } else {
                  allSucceeded = false
                }
              }
              if (allSucceeded) {
                if (transitions == null) {
                  transitions = AutomaTreeNodeFilter.instantiateIntObjectMap[ArrayBuffer[resAutomaton.AutomatonNode]]
                }
                transitions.put(cursor.key(), createdSuc)
              }
            } else {
              interType match {
                case IntersectType.BBtoBB => {
                  throw new RuntimeException("not good")
                }
                case IntersectType.FFtoFB => {
                  var createdSuc: ArrayBuffer[resAutomaton.AutomatonNode] = null
                  var allSucceeded = true
                  for (i <- c1.size - 1 to 0 by -1 if allSucceeded) {
                    val childrenInter = intersectDfstratPar(resAutomaton)(c1(i), if (i == c1.size - 1) null else c2(i), interType, nodeBuffer, if (i == c1.size - 1) c2(c2.size - 1) else null, -1, revuz)
                    if (childrenInter != null) {
                      if (createdSuc == null) {
                        createdSuc = ArrayBuffer.fill(c1.size)(null)
                      }
                      createdSuc(i) = childrenInter
                    } else {
                      allSucceeded = false
                    }
                  }
                  if (allSucceeded) {
                    if (transitions == null) {
                      transitions = AutomaTreeNodeFilter.instantiateIntObjectMap[ArrayBuffer[resAutomaton.AutomatonNode]]
                    }
                    transitions.put(cursor.key(), createdSuc)
                  }
                }
                case IntersectType.BFtoBF => {
                  var createdSuc: ArrayBuffer[resAutomaton.AutomatonNode] = ArrayBuffer.fill(c2.size)(null)
                  var allSucceeded = true
                  var extraFromCache = false
                  var extraKey = 0L
                  for (i <- c2.size - 1 to 0 by -1 if allSucceeded) {
                    if (i == c2.size - 1) {
                      extraKey = -(c2(i).id.longValue())
                      val extraTrans = if (nodeBuffer.containsKey(extraKey)) {
                        extraFromCache = true
                        nodeBuffer.get(extraKey)
                      } else {
                        resAutomaton.newLeafNode(c2(i).codeVertexId, i, c2(i).transitions.keySet())
                      }
                      createdSuc(i) = extraTrans
                    } else {
                      val childrenInter = intersectDfstratPar(resAutomaton)(c1(i), c2(i), interType, nodeBuffer, null, doubleFFsource, revuz)
                      if (childrenInter != null) {
                        createdSuc(i) = childrenInter
                      } else {
                        allSucceeded = false
                      }
                    }
                  }
                  if (allSucceeded) {
                    if (!extraFromCache) {
                      val k = (createdSuc(createdSuc.size - 1).transitions, Nil)
                      val existing = revuz(createdSuc(createdSuc.size - 1).codeVertexId).putIfAbsent(k, createdSuc(createdSuc.size - 1))
                      if (existing == null) {
                      } else {
                        createdSuc(createdSuc.size - 1) = existing
                      }
                      nodeBuffer.put(extraKey, createdSuc(createdSuc.size - 1))
                    }
                    if (transitions == null) {
                      transitions = AutomaTreeNodeFilter.instantiateIntObjectMap[ArrayBuffer[resAutomaton.AutomatonNode]]
                    }
                    transitions.put(cursor.key(), createdSuc)
                  }
                }
                case IntersectType.FFtoFF => {
                  val maxSize = Math.max(if (c1 == null) 0 else c1.size, if (c2 == null) 0 else c2.size)
                  val createdSuc: ArrayBuffer[resAutomaton.AutomatonNode] = ArrayBuffer.fill(maxSize)(null)
                  var allSucceeded = true
                  var extraFromCache = false
                  var extraKey = 0L
                  for (i <- maxSize - 1 to 0 by -1 if allSucceeded) {
                    if (i == maxSize - 1) {
                      extraKey = if (c1 == null || (c2 != null && c2.size > c1.size)) -(c2(i).id.longValue()) else -((c1(i).id.longValue()) << 32)
                      val extraTrans = if (nodeBuffer.containsKey(extraKey)) {
                        extraFromCache = true
                        nodeBuffer.get(extraKey)
                      } else {
                        if (c1 == null || (c2 != null && c2.size > c1.size)) {
                          resAutomaton.newNode(c2(i).codeVertexId + 1, i, c2(i).transitions, null)
                        } else {
                          resAutomaton.newNode(c1(i).codeVertexId, i, c1(i).transitions, null)
                        }
                      }
                      createdSuc(i) = extraTrans
                    } else {
                      val childrenInter = intersectDfstratPar(resAutomaton)(c1(i), c2(i), interType, nodeBuffer, null, doubleFFsource, revuz)
                      if (childrenInter != null) {
                        createdSuc(i) = childrenInter
                      } else {
                        allSucceeded = false
                      }
                    }
                  }
                  if (allSucceeded) {
                    if (!extraFromCache) {
                      val k = (createdSuc(createdSuc.size - 1).transitions, Nil)
                      val existing = revuz(createdSuc(createdSuc.size - 1).codeVertexId).putIfAbsent(k, createdSuc(createdSuc.size - 1))
                      if (existing == null) {
                      } else {
                        createdSuc(createdSuc.size - 1) = existing
                      }
                      nodeBuffer.put(extraKey, createdSuc(createdSuc.size - 1))
                    }
                    if (transitions == null) {
                      transitions = AutomaTreeNodeFilter.instantiateIntObjectMap[ArrayBuffer[resAutomaton.AutomatonNode]]
                    }
                    transitions.put(cursor.key(), createdSuc)
                  }
                }
              }
            }
          }
        }
      }
    }
    if (transitions == null) {
      nodeBuffer.put(key, resAutomaton.root)
      return null
    } else {
      val k = (transitions, filter)
      val existing = revuz(n1.codeVertexId).get(k)
      val intersectionRes = if (existing == null) {
        val n = resAutomaton.newNode(n1.codeVertexId, n1.childPos, transitions)
        n.backEdgeSource = n1.backEdgeSource || (interType == IntersectType.FFtoFB && n.codeVertexId == resAutomaton.nbNodes - 1)
        n.conditionalTransitions = filter
        val revuzRes = revuz(n.codeVertexId).putIfAbsent(k, n)
        if (revuzRes == null) {
          n
        } else {
          revuzRes
        }
      } else {
        existing
      }
      nodeBuffer.put(key, intersectionRes)
      return intersectionRes
    }
  }

  private def extendImp(resAutomaton: AutomaTreeNodeFilter)(n: AutomaTreeNodeFilter#AutomatonNode, extensions: IntObjMap[_ <: ArrayBuffer[_ <: AutomaTreeNodeFilter#AutomatonNode]], thisBuffer: IntObjMap[resAutomaton.AutomatonNode], leavesBuffer: IntObjMap[resAutomaton.AutomatonNode], revuz: Array[UnifiedMap[(IntObjMap[ArrayBuffer[resAutomaton.AutomatonNode]], List[(Int, fr.liglab.sami.utilities.Filter)]), resAutomaton.AutomatonNode]]): resAutomaton.AutomatonNode = {
    if (thisBuffer.containsKey(n.id)) {
      return thisBuffer.get(n.id)
    }
    var transitions: IntObjMap[ArrayBuffer[resAutomaton.AutomatonNode]] = null
    val filter = n.conditionalTransitions
    if (n.codeVertexId != resAutomaton.nbNodes - 2) {
      val cursor = n.transitions.cursor()
      while (cursor.moveNext()) {
        val successors = cursor.value()
        if (successors != null) {
          var createdSuc: ArrayBuffer[resAutomaton.AutomatonNode] = null
          var allSucceeded = true
          for (i <- 0 until successors.size if allSucceeded) {
            val rec = extendImp(resAutomaton)(successors(i), extensions, thisBuffer, leavesBuffer, revuz)
            if (rec == null) {
              allSucceeded = false
            } else {
              if (createdSuc == null) createdSuc = new ArrayBuffer(successors.size)
              createdSuc += rec
            }
          }
          if (allSucceeded) {
            if (transitions == null) {
              transitions = AutomaTreeNodeFilter.instantiateIntObjectMap[ArrayBuffer[resAutomaton.AutomatonNode]](n.transitions.size())
            }
            transitions.put(cursor.key(), createdSuc)
          }
        } else {
          if (transitions == null) {
            transitions = new FakeMap(AutomaTreeNodeFilter.instantiateIntSet(n.transitions.size()), null)
          }
          transitions.put(cursor.key(), null)
        }
      }
    } else {
      val cursor = n.transitions.cursor()
      while (cursor.moveNext()) {
        val leaf = extensions.get(cursor.key())
        if (leaf != null) {
          var leafNode = leavesBuffer.get(leaf(0).id)
          if (leafNode == null) {
            leafNode = resAutomaton.newLeafNode(resAutomaton.nbNodes - 1, 0, leaf(0).transitions.keySet())
            leavesBuffer.put(leaf(0).id, leafNode)
          }
          if (transitions == null) {
            transitions = AutomaTreeNodeFilter.instantiateIntObjectMap[ArrayBuffer[resAutomaton.AutomatonNode]](n.transitions.size())
          }
          val ab = new ArrayBuffer[resAutomaton.AutomatonNode](1)
          ab += leafNode
          transitions.put(cursor.key, ab)
        }

      }
    }
    if (transitions != null) {
      val k = (transitions, filter)
      val existing = revuz(n.codeVertexId).get(k)
      val extensionRes = if (existing == null) {
        val e = resAutomaton.newNode(n.codeVertexId, n.childPos, transitions)
        e.backEdgeSource = n.backEdgeSource
        e.conditionalTransitions = filter
        revuz(n.codeVertexId).put(k, e)
        e
      } else {
        existing
      }
      thisBuffer.put(n.id, extensionRes)
      return extensionRes
    } else {
      thisBuffer.put(n.id, null)
      return null
    }
  }

  private def extendImpPar(resAutomaton: AutomaTreeNodeFilter)(n: AutomaTreeNodeFilter#AutomatonNode, extensions: IntObjMap[_ <: ArrayBuffer[_ <: AutomaTreeNodeFilter#AutomatonNode]], thisBuffer: ConcurrentHashMap[Integer, resAutomaton.AutomatonNode], leavesBuffer: ConcurrentHashMap[Integer, resAutomaton.AutomatonNode], revuz: Array[ConcurrentHashMap[(IntObjMap[ArrayBuffer[resAutomaton.AutomatonNode]], List[(Int, fr.liglab.sami.utilities.Filter)]), resAutomaton.AutomatonNode]]): resAutomaton.AutomatonNode = {
    val inNodeBuffer = thisBuffer.get(n.id)
    if (inNodeBuffer == resAutomaton.root) return null else if (inNodeBuffer != null) return inNodeBuffer
    var transitions: IntObjMap[ArrayBuffer[resAutomaton.AutomatonNode]] = null
    val filter = n.conditionalTransitions
    if (n.codeVertexId != resAutomaton.nbNodes - 2) {
      val cursor = n.transitions.cursor()
      while (cursor.moveNext()) {
        val successors = cursor.value()
        if (successors != null) {
          var createdSuc: ArrayBuffer[resAutomaton.AutomatonNode] = null
          var allSucceeded = true
          for (i <- 0 until successors.size if allSucceeded) {
            val rec = extendImpPar(resAutomaton)(successors(i), extensions, thisBuffer, leavesBuffer, revuz)
            if (rec == null) {
              allSucceeded = false
            } else {
              if (createdSuc == null) createdSuc = new ArrayBuffer(successors.size)
              createdSuc += rec
            }
          }
          if (allSucceeded) {
            if (transitions == null) {
              transitions = AutomaTreeNodeFilter.instantiateIntObjectMap[ArrayBuffer[resAutomaton.AutomatonNode]](n.transitions.size())
            }
            transitions.put(cursor.key(), createdSuc)
          }
        } else {
          if (transitions == null) {
            transitions = new FakeMap(AutomaTreeNodeFilter.instantiateIntSet(n.transitions.size()), null)
          }
          transitions.put(cursor.key(), null)
        }
      }
    } else {
      val cursor = n.transitions.cursor()
      while (cursor.moveNext()) {
        val leaf = extensions.get(cursor.key())
        if (leaf != null) {
          var leafNode = leavesBuffer.get(leaf(0).id)
          if (leafNode == null) {
            leafNode = resAutomaton.newLeafNode(resAutomaton.nbNodes - 1, 0, leaf(0).transitions.keySet())
            val leavesPutRes = leavesBuffer.putIfAbsent(leaf(0).id, leafNode)
            if (leavesPutRes == null) {
            } else {
              leafNode = leavesPutRes
            }
          }
          if (transitions == null) {
            transitions = AutomaTreeNodeFilter.instantiateIntObjectMap[ArrayBuffer[resAutomaton.AutomatonNode]](n.transitions.size())
          }
          val ab = new ArrayBuffer[resAutomaton.AutomatonNode](1)
          ab += leafNode
          transitions.put(cursor.key, ab)
        }

      }
    }
    if (transitions != null) {
      val k = (transitions, filter)
      val existing = revuz(n.codeVertexId).get(k)
      val extensionRes = if (existing == null) {
        val e = resAutomaton.newNode(n.codeVertexId, n.childPos, transitions)
        e.backEdgeSource = n.backEdgeSource
        e.conditionalTransitions = filter
        val revuzRes = revuz(n.codeVertexId).putIfAbsent(k, e)
        if (revuzRes == null) {
          e
        } else {
          revuzRes
        }
      } else {
        existing
      }
      thisBuffer.put(n.id, extensionRes)
      return extensionRes
    } else {
      thisBuffer.put(n.id, resAutomaton.root)
      return null
    }
  }

  //  //  var recCounter = 0
  private def projectImp(resAutomaton: AutomaTreeNodeFilter)(n: AutomaTreeNodeFilter#AutomatonNode, filter: Array[IntSet], thisBuffer: IntObjMap[resAutomaton.AutomatonNode], revuz: Array[UnifiedMap[(IntObjMap[ArrayBuffer[resAutomaton.AutomatonNode]], List[(Int, fr.liglab.sami.utilities.Filter)]), resAutomaton.AutomatonNode]]): resAutomaton.AutomatonNode = {
    //    recCounter += 1
    //    if(recCounter %1000 == 0) println("nbRec: " + recCounter + " size " + thisBuffer.size())
    if (thisBuffer.containsKey(n.id)) {
      return thisBuffer.get(n.id)
    }
    var transitions: IntObjMap[ArrayBuffer[resAutomaton.AutomatonNode]] = null
    val cursor = n.transitions.cursor()
    while (cursor.moveNext()) {
      if (filter.size == n.codeVertexId || filter(n.codeVertexId).contains(cursor.key())) {
        val successors = cursor.value()
        if (successors != null) {
          var createdSuc: ArrayBuffer[resAutomaton.AutomatonNode] = null
          var allSucceeded = true
          for (i <- 0 until successors.size if allSucceeded) {
            val rec = projectImp(resAutomaton)(successors(i), filter, thisBuffer, revuz)
            if (rec == null) {
              allSucceeded = false
            } else {
              if (createdSuc == null) createdSuc = new ArrayBuffer(successors.size)
              createdSuc += rec
            }
          }
          if (allSucceeded) {
            if (transitions == null) {
              transitions = AutomaTreeNodeFilter.instantiateIntObjectMap[ArrayBuffer[resAutomaton.AutomatonNode]](n.transitions.size())
            }
            transitions.put(cursor.key(), createdSuc)
          }
        } else {
          if (transitions == null) {
            transitions = new FakeMap(AutomaTreeNodeFilter.instantiateIntSet(n.transitions.size()), null)
          }
          transitions.put(cursor.key(), null)
        }
      }
    }
    if (transitions != null) {
      val befilter = n.conditionalTransitions
      val k = (transitions, befilter)
      val existing = revuz(n.codeVertexId).get(k)
      val extensionRes = if (existing == null) {
        val e = resAutomaton.newNode(n.codeVertexId, n.childPos, transitions)
        e.backEdgeSource = n.backEdgeSource
        e.conditionalTransitions = befilter
        revuz(n.codeVertexId).put(k, e)
        e
      } else {
        existing
      }
      thisBuffer.put(n.id, extensionRes)
      return extensionRes
    } else {
      thisBuffer.put(n.id, null)
      return null
    }
  }

  private def projectImpPar(resAutomaton: AutomaTreeNodeFilter)(n: AutomaTreeNodeFilter#AutomatonNode, filter: Array[IntSet], thisBuffer: ConcurrentHashMap[Integer, resAutomaton.AutomatonNode], revuz: Array[ConcurrentHashMap[(IntObjMap[ArrayBuffer[resAutomaton.AutomatonNode]], List[(Int, fr.liglab.sami.utilities.Filter)]), resAutomaton.AutomatonNode]]): resAutomaton.AutomatonNode = {
    //    recCounter += 1
    //    if(recCounter %1000 == 0) println("nbRec: " + recCounter + " size " + thisBuffer.size())
    val inNodeBuffer = thisBuffer.get(n.id)
    if (inNodeBuffer == resAutomaton.root) return null else if (inNodeBuffer != null) return inNodeBuffer
    var transitions: IntObjMap[ArrayBuffer[resAutomaton.AutomatonNode]] = null
    val cursor = n.transitions.cursor()
    while (cursor.moveNext()) {
      if (filter.size == n.codeVertexId || filter(n.codeVertexId).contains(cursor.key())) {
        val successors = cursor.value()
        if (successors != null) {
          var createdSuc: ArrayBuffer[resAutomaton.AutomatonNode] = null
          var allSucceeded = true
          for (i <- 0 until successors.size if allSucceeded) {
            val rec = projectImpPar(resAutomaton)(successors(i), filter, thisBuffer, revuz)
            if (rec == null) {
              allSucceeded = false
            } else {
              if (createdSuc == null) createdSuc = new ArrayBuffer(successors.size)
              createdSuc += rec
            }
          }
          if (allSucceeded) {
            if (transitions == null) {
              transitions = AutomaTreeNodeFilter.instantiateIntObjectMap[ArrayBuffer[resAutomaton.AutomatonNode]](n.transitions.size())
            }
            transitions.put(cursor.key(), createdSuc)
          }
        } else {
          if (transitions == null) {
            transitions = new FakeMap(AutomaTreeNodeFilter.instantiateIntSet(n.transitions.size()), null)
          }
          transitions.put(cursor.key(), null)
        }
      }
    }
    if (transitions != null) {
      val befilter = n.conditionalTransitions
      val k = (transitions, befilter)
      val existing = revuz(n.codeVertexId).get(k)
      val extensionRes = if (existing == null) {
        val e = resAutomaton.newNode(n.codeVertexId, n.childPos, transitions)
        e.backEdgeSource = n.backEdgeSource
        e.conditionalTransitions = befilter
        val revuzRes = revuz(n.codeVertexId).putIfAbsent(k, e)
        if (revuzRes == null) {
          e
        } else {
          revuzRes
        }
      } else {
        return existing
      }
      //revuz has ensured no race condition problem already, same key implies same value
      thisBuffer.put(n.id, extensionRes)
      return extensionRes
    } else {
      thisBuffer.put(n.id, resAutomaton.root)
      return null
    }
  }

  def groupExtendedTransitions(resAutomaton: AutomaTreeNodeFilter)(extensions: IntObjMap[IntSet]): IntObjMap[ArrayBuffer[resAutomaton.AutomatonNode]] = {
    val res: IntObjMap[ArrayBuffer[resAutomaton.AutomatonNode]] = HashIntObjMaps.newMutableMap(extensions.size)
    val byNbSuccessors: IntObjMap[IntArrayList] = AutomaTreeNodeFilter.instantiateIntObjectMap
    val cursor = extensions.cursor()
    while (cursor.moveNext()) {
      var r = byNbSuccessors.get(cursor.value().size())
      if (r == null) {
        r = new IntArrayList
        byNbSuccessors.put(cursor.value().size, r)
      }
      r.add(cursor.key())
    }
    val sortedNbSucc = byNbSuccessors.keySet().toIntArray().sortBy(-_)
    val reusableTransitions: UnifiedMap[IntSet, Integer] = new UnifiedMap
    for (s <- sortedNbSucc) {
      val candidates = byNbSuccessors.get(s)
      if (s == 1) {
        val cur = candidates.intIterator()
        while (cur.hasNext()) {
          val c = cur.next()
          val suc = extensions.get(c)
          val copy = reusableTransitions.getOrDefault(suc, -1).toInt
          if (copy != -1) {
            res.put(c, res.get(copy))
          } else {
            val ab: ArrayBuffer[resAutomaton.AutomatonNode] = new ArrayBuffer(1)
            ab += resAutomaton.newLeafNode(1, 0, suc)
            res.put(c, ab)
            reusableTransitions.put(suc, c)
          }
        }
      } else {
        if (candidates.size == 1) {
          val cur = candidates.intIterator()
          val c = cur.next()
          val suc = extensions.get(c)
          var node: ArrayBuffer[resAutomaton.AutomatonNode] = null
          if (byNbSuccessors.containsKey(s + 1)) {
            suc.add(c)
            val copy = reusableTransitions.getOrDefault(suc, -1).toInt
            if (copy == -1) {
              suc.remove(c)
            } else {
              node = res.get(copy)
            }
          }
          if (node == null) {
            node = new ArrayBuffer(1)
            node += resAutomaton.newLeafNode(1, 0, suc)
            if (s != 2 && byNbSuccessors.containsKey(s - 1)) {
              reusableTransitions.put(suc, c)
            }
          }
          res.put(c, node)
        } else {
          val cur = candidates.intIterator()
          val tempReusable: UnifiedMap[IntSet, Integer] = new UnifiedMap
          while (cur.hasNext()) {
            val c = cur.next()
            val suc = extensions.get(c)
            suc.add(c)
            var node: ArrayBuffer[resAutomaton.AutomatonNode] = null
            if (byNbSuccessors.containsKey(s + 1)) {
              val copy = reusableTransitions.getOrDefault(suc, -1).toInt
              if (copy != -1) {
                node = res.get(copy)
                res.put(c, node)
              }
            }
            if (node == null) {
              val copy = tempReusable.getOrDefault(suc, -1).toInt
              if (copy != -1) {
                node = res.get(copy)
                if (node == null) {
                  node = new ArrayBuffer(1)
                  node += resAutomaton.newLeafNode(1, 0, suc)
                  res.put(copy, node)
                  res.put(c, node)
                } else {
                  node = res.get(copy)
                  res.put(c, node)
                }
              } else {
                tempReusable.put(suc, c)
              }
            }
          }
          val cur2 = candidates.intIterator()
          while (cur2.hasNext()) {
            val c = cur2.next()
            if (!res.containsKey(c)) {
              val suc = extensions.get(c)
              suc.remove(c)
              val copy = reusableTransitions.getOrDefault(c, -1).toInt
              if (copy != -1) {
                res.put(c, res.get(copy))
              } else {
                val ab: ArrayBuffer[resAutomaton.AutomatonNode] = new ArrayBuffer(1)
                ab += resAutomaton.newLeafNode(1, 0, suc)
                res.put(c, ab)
                reusableTransitions.put(suc, c)
              }
            }
          }
        }
      }
    }
    //    val sortedMap = new SortedListMap[ArrayBuffer[resAutomaton.AutomatonNode]](res.size())
    //    for (k <- res.keySet().toIntArray().sorted) {
    //      sortedMap.put(k, res.get(k))
    //    }
    //    return sortedMap
    return res
  }

  def main(args: Array[String]) {
    val m = new IntIntHashMap()
    println(m.get(0))
    //    val d1: AutomaTreeForwardRef = new AutomaTreeForwardRef(List((1, 3), (1, 4), (2, 3), (2, 5)), false)
    //    val d2: AutomaTreeForwardRef = new AutomaTreeForwardRef(List((1, 3), (2, 3)), false)
    //    //    val extensions: Map[Int, Seq[Int]] = Map(3 -> Array(6), 5 -> Array(7))
    //    println("starting intersection")
    //    val d3 = d1.intersectFFtoFF(d2, 0, -1)
    //    println("sup " + d3.MNISupport)
    //    for (p <- d3.paths) {
    //      println(p)
    //    }
    //    println("starting extension")
    //    val d3 = d1.extend(extensions)
    //    println("sup " + d3.MNISupport)
    //    for (p <- d3.paths) {
    //      println(p)
    //    }
  }
}
