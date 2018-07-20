package fr.liglab.sami.code

import java.io.IOException
import java.io.ObjectInputStream
import java.io.ObjectOutputStream
import java.util.TreeSet

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.ListBuffer

import org.eclipse.collections.impl.list.mutable.primitive.IntArrayList
import org.eclipse.collections.impl.map.mutable.UnifiedMap
import org.eclipse.collections.impl.map.mutable.primitive.IntIntHashMap

import fr.liglab.sami.code.IntersectType.IntersectType
import fr.liglab.sami.utilities.FakeMap
import fr.liglab.sami.utilities.StopWatch
import net.openhft.koloboke.collect.map.IntIntMap
import net.openhft.koloboke.collect.map.IntObjMap
import net.openhft.koloboke.collect.map.LongObjMap
import net.openhft.koloboke.collect.map.ObjObjMap
import net.openhft.koloboke.collect.map.hash.HashIntIntMapFactory
import net.openhft.koloboke.collect.map.hash.HashIntIntMaps
import net.openhft.koloboke.collect.map.hash.HashIntObjMap
import net.openhft.koloboke.collect.map.hash.HashIntObjMapFactory
import net.openhft.koloboke.collect.map.hash.HashIntObjMaps
import net.openhft.koloboke.collect.map.hash.HashLongObjMaps
import net.openhft.koloboke.collect.map.hash.HashObjObjMap
import net.openhft.koloboke.collect.map.hash.HashObjObjMaps
import net.openhft.koloboke.collect.set.IntSet
import net.openhft.koloboke.collect.set.hash.HashIntSets

//implementation based on automata principles
@SerialVersionUID(1L)
class AutomaTree(var nbNodes: Int, var parallelValidation: Boolean) extends DAG with Serializable {
  //  private var debug = false
  //  private var preDebug = false
  private var nodeIdDistributor = 1
  private var root: AutomatonNode = null
  private var nodesByDepth: Array[mutable.Buffer[AutomatonNode]] = null
  private var MNISupportCached: Int = -1
  private var transByLevel: Array[IntSet] = null

  def this(instances: Iterable[(Int, Int)], parallelValidation: Boolean) {
    this(2, parallelValidation)
    val vToSuccessors: IntObjMap[IntSet] = AutomaTree.instantiateIntObjectMap[IntSet]
    instances.foreach {
      p =>
        var suc = vToSuccessors.get(p._1)
        if (suc == null) {
          suc = AutomaTree.instantiateIntSet
          vToSuccessors.put(p._1, suc)
        }
        suc.add(p._2)
    }
    val transitions = AutomaTree.groupExtendedTransitions(this)(vToSuccessors)
    this.root = this.newNode(0, 0, transitions)
  }

  def this(vToSuccessors: IntObjMap[IntSet], parallelValidation: Boolean) {
    this(2, parallelValidation)
    val transitions = AutomaTree.groupExtendedTransitions(this)(vToSuccessors)
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

  private def getNextNodeId: Int = {
    val id = this.nodeIdDistributor
    this.nodeIdDistributor = this.nodeIdDistributor + 1
    if (id < 0) throw new RuntimeException("ran out of IDs")
    //    println("giving id " + id + " next " + this.nodeIdDistributor)
    return id
  }
  private def getMNISupport(): Int = {
    if (this.root == null) {
      this.MNISupportCached = 0
    } else if (this.MNISupportCached == -1) {
      val distinctValsByDepth: Array[IntSet] = Array.fill(this.nbNodes)(AutomaTree.instantiateIntSet)
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

  def getTransByLevel(): Array[IntSet] = {
    if (this.transByLevel == null) {
      this.synchronized {
        this.transByLevel = Array.fill(this.nbNodes)(AutomaTree.instantiateIntSet)
        this.applyToEachDagNode(n => transByLevel(n.codeVertexId).addAll(n.transitions.keySet()))
      }
    }
    return this.transByLevel
  }
  private def getLeafSet(): IntSet = {
    var leafSet = AutomaTree.instantiateIntSet
    def updateLeafSetFunction(n: AutomaTree#AutomatonNode) = {
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
    def recPaths(stack: List[AutomaTree#AutomatonNode]): Unit = {
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
    val p = AutomaTree.instantiateIntSet
    var count = 0L
    def recPaths(stack: List[AutomaTree#AutomatonNode]): Unit = {
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
    def recPaths(stack: List[AutomaTree#AutomatonNode]): Unit = {
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
    val visited: IntSet = AutomaTree.instantiateIntSet
    recApplyToEachDagNode(this.root, f)
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
    val resAutomaton = new AutomaTree(this.nbNodes + 1, this.parallelValidation)
    //    try {
    val d2 = other.asInstanceOf[AutomaTree]
    resAutomaton.nodesByDepth = Array.fill(resAutomaton.nbNodes)(mutable.ArrayBuffer.empty)
    val extensions: IntObjMap[_ <: ArrayBuffer[_ <: AutomaTree#AutomatonNode]] = d2.root.transitions
    resAutomaton.root = AutomaTree.extend(resAutomaton)(this.root, extensions, threshold)
    //    val mniSets: Array[IntSet] = Array.fill(resAutomaton.nbNodes)(HashIntSets.newMutableSet())
    //    resAutomaton.mniSetsFromEmbeddings(mniSets)
    //    val refMniSupport = mniSets.map(_.size()).min
    //    for (d <- resAutomaton.nodesByDepth) {
    //      println("#nodes " + d.size)
    //    }
    //    println("starting validation")
    resAutomaton.validateTransitionsAndMinimize(true, threshold)
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
    val resAutomaton = new AutomaTree(this.nbNodes, this.parallelValidation)
    //    try {
    val d2 = other.asInstanceOf[AutomaTree]
    resAutomaton.nodesByDepth = Array.fill(resAutomaton.nbNodes)(mutable.ArrayBuffer.empty)
    resAutomaton.root = AutomaTree.intersect(resAutomaton)(this.root, d2.root, IntersectType.BBtoBB, threshold, -1)
    resAutomaton.validateTransitionsAndMinimize(false, threshold)
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
    val resAutomaton = new AutomaTree(this.nbNodes + 1, this.parallelValidation)
    //    try {
    val d2 = other.asInstanceOf[AutomaTree]
    //    this.describe
    //    d2.describe
    resAutomaton.nodesByDepth = Array.fill(resAutomaton.nbNodes)(mutable.ArrayBuffer.empty)
    resAutomaton.root = AutomaTree.intersect(resAutomaton)(this.root, d2.root, IntersectType.BFtoBF, threshold, -1)
    //    resAutomaton.describe
    resAutomaton.validateTransitionsAndMinimize(false, threshold)
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
    val resAutomaton = new AutomaTree(this.nbNodes + 1, this.parallelValidation)
    //    try {
    val d2 = other.asInstanceOf[AutomaTree]
    //    this.getNodesByLevel().foreach(l => println(l.size))
    //    d2.getNodesByLevel().foreach(l => println(l.size))
    resAutomaton.nodesByDepth = Array.fill(resAutomaton.nbNodes)(mutable.ArrayBuffer.empty)
    //      println("building dag " + System.currentTimeMillis())
    resAutomaton.root = AutomaTree.intersect(resAutomaton)(this.root, d2.root, IntersectType.FFtoFF, threshold, doubleFFPos)
    //    val mniSets: Array[IntSet] = Array.fill(resAutomaton.nbNodes)(HashIntSets.newMutableSet())
    //    resAutomaton.mniSetsFromEmbeddings(mniSets)
    //    val refMniSupport = mniSets.map(_.size()).min
    //    println("child " + resAutomaton.hasEmbedding(List(2680084, 2737777, 2059346, 2479691, 2023408, 2723979, 2306538)))
    //            println("going into validation")
    //    for (d <- resAutomaton.nodesByDepth) {
    //      println("#nodes " + d.size)
    //    }
    //    println("starting validation")
    resAutomaton.validateTransitionsAndMinimize(false, threshold)
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
    //    println(resAutomaton.MNISupport)
    return resAutomaton
  }
  override def intersectFFtoFB(other: DAG, threshold: Int, lazyCleanup: Boolean, automorph: Array[List[Int]], useProjection: Boolean): DAG = {
    val resAutomaton = new AutomaTree(this.nbNodes, this.parallelValidation)
    val d2 = other.asInstanceOf[AutomaTree]
    //    println("this states and trans " + this.getNbStatesAndTransitions())
    //    println("other states and trans " + d2.getNbStatesAndTransitions())
    var sw = new StopWatch()
    sw.start()
    if (useProjection) {
      val d1proj = new AutomaTree(this.nbNodes, this.parallelValidation)
      d1proj.root = AutomaTree.project(d1proj)(this.root, d2.getTransByLevel(), threshold)
      //            d1proj.describe
      if (d1proj.root == null) return resAutomaton
      val d2proj = new AutomaTree(d2.nbNodes, d2.parallelValidation)
      d2proj.root = AutomaTree.project(d2proj)(d2.root, this.getTransByLevel(), threshold)
      //            d2proj.describe
      if (d2proj.root == null) return resAutomaton
      println("done projection " + sw.getElapsedTime())
      resAutomaton.nodesByDepth = Array.fill(resAutomaton.nbNodes)(mutable.ArrayBuffer.empty)
      resAutomaton.root = AutomaTree.intersect(resAutomaton)(d1proj.root, d2proj.root, IntersectType.FFtoFB, threshold, -1)
    } else {
      resAutomaton.nodesByDepth = Array.fill(resAutomaton.nbNodes)(mutable.ArrayBuffer.empty)
      resAutomaton.root = AutomaTree.intersect(resAutomaton)(this.root, d2.root, IntersectType.FFtoFB, threshold, -1)
    }
    //    println("going into validation")
    //    for (d <- resAutomaton.nodesByDepth) {
    //      println("#nodes " + d.size)
    //    }
    //    println("starting validation " + sw.getElapsedTime())
    resAutomaton.validateTransitionsAndMinimize(false, threshold)
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

  private def validateTransitionsAndMinimize(cleanupLeaves: Boolean, threshold: Int): Unit = {
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
      var levels: List[mutable.Set[AutomatonNode]] = Nil
      val mniSet = AutomaTree.instantiateIntSet
      this.MNISupportCached = Int.MaxValue
      if (this.parallelValidation) {

        val consumedBuffer: ThreadLocal[IntIntMap] = new ThreadLocal[IntIntMap] {
          override protected def initialValue(): IntIntMap = {
            AutomaTree.instantiateIntIntMap(nbNodes - 1)
          }
        }

        val knownBuffer: ThreadLocal[IntIntMap] = new ThreadLocal[IntIntMap] {
          override protected def initialValue(): IntIntMap = {
            AutomaTree.instantiateIntIntMap(nbNodes - 1)
          }
        }

        val blockedPaths: ThreadLocal[IntIntMap] = new ThreadLocal[IntIntMap] {
          override protected def initialValue(): IntIntMap = {
            AutomaTree.instantiateIntIntMap
          }
        }
        val releasePaths: ThreadLocal[Array[IntSet]] = new ThreadLocal[Array[IntSet]] {
          override protected def initialValue(): Array[IntSet] = {
            Array.fill(nbNodes + 1)(AutomaTree.instantiateIntSet)
          }
        }

        val lockLevels: ThreadLocal[Array[TreeSet[Int]]] = new ThreadLocal[Array[TreeSet[Int]]] {
          override protected def initialValue(): Array[TreeSet[Int]] = {
            Array.fill(nbNodes)(new TreeSet)
          }
        }
        //        println("parallel validation")
        def recValidate(toDo: List[mutable.Set[AutomatonNode]], firstExec: Boolean): Boolean = {
          if (toDo.isEmpty) return true
          val done = toDo.head
          val nextGen: ArrayBuffer[mutable.Set[AutomatonNode]] = new ArrayBuffer()
          if (!firstExec) levels = done :: levels
          done.par.foreach(n => n.validateVertices(mniSet, consumedBuffer.get, knownBuffer.get, blockedPaths.get, releasePaths.get, lockLevels.get))
          this.MNISupportCached = Math.min(this.MNISupportCached, mniSet.size())
          if (this.MNISupportCached < threshold) {
            this.MNISupportCached = 0
            this.root = null
            return false
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
        val consumedBuffer: IntIntMap = AutomaTree.instantiateIntIntMap(nbNodes - 1)
        val konwnBuffer: IntIntMap = AutomaTree.instantiateIntIntMap(nbNodes - 1)
        val blockedPaths: IntIntMap = AutomaTree.instantiateIntIntMap
        val releasePaths: Array[IntSet] = Array.fill(nbNodes + 1)(AutomaTree.instantiateIntSet)
        val lockLevels: Array[TreeSet[Int]] = Array.fill(nbNodes)(new TreeSet)
        def recValidate(toDo: List[mutable.Set[AutomatonNode]], firstExec: Boolean): Boolean = {
          if (toDo.isEmpty) return true
          val done = toDo.head
          val nextGen: ArrayBuffer[mutable.Set[AutomatonNode]] = new ArrayBuffer()
          if (!firstExec) levels = done :: levels
          for (n <- done) {
            //                        val sw = new StopWatch
            //                        sw.start
            n.validateVertices(mniSet, consumedBuffer, konwnBuffer, blockedPaths, releasePaths, lockLevels)
            //                        if (sw.getElapsedTime() > 100) println(sw.getElapsedTime() + "\t" + n)
          }
          this.MNISupportCached = Math.min(this.MNISupportCached, mniSet.size())
          if (this.MNISupportCached < threshold) {
            this.MNISupportCached = 0
            this.root = null
            return false
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

      while (!levels.isEmpty) {
        this.mergeLevel(levels.head)
        levels = levels.tail
      }
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

  def newNode(codeVertexId: Int, childPos: Int, m: IntObjMap[ArrayBuffer[AutomatonNode]]): AutomatonNode = {
    new AutomatonNode(codeVertexId, childPos, m)
  }

  def mergeLevel(level: Iterable[AutomatonNode]) = {
    //if it has a single parent try to merge more?
    //    println("level " + level)
    val groupedBySizeAndNbSuc: LongObjMap[List[AutomatonNode]] = HashLongObjMaps.newMutableMap()
    for (n <- level) {
      if (n.transitions != null) {
        val key: Long = if (n.successors == null) {
          n.transitions.size.longValue()
        } else {
          (n.transitions.size.longValue() << 32) | (n.successors.size.longValue() & 0xffffffffL)
        }
        val l = groupedBySizeAndNbSuc.get(key)
        if (l == null) {
          groupedBySizeAndNbSuc.put(key, List(n))
        } else {
          groupedBySizeAndNbSuc.put(key, n :: l)
        }
      }
    }
    val c = groupedBySizeAndNbSuc.cursor()
    while (c.moveNext()) {
      val l = c.value()
      if (!l.tail.isEmpty) {
        //ObjObjMap was slow when there were many nodes
        val collisionMap: UnifiedMap[IntObjMap[ArrayBuffer[AutomatonNode]], AutomatonNode] = new UnifiedMap(l.size)
        for (n <- l) {
          val existing = collisionMap.putIfAbsent(n.transitions, n)
          if (existing != null) {
            //            println("merge")
            mergeNodes(n, existing)
          } else {
            //            println("no merge")
          }
        }
      }
    }
  }

  private final def mergeNodes(n: AutomatonNode, existing: AutomatonNode) = {
    //    println("merge " + n + " into " + existing + " " + n.codeVertexId + " " + existing.codeVertexId)
    val predCursor = n.predecessors.cursor()
    //    println(n.predecessors)
    while (predCursor.moveNext) {
      //      println(predCursor.key().successors(n.childPos).get(n))
      val pred = predCursor.key()
      val c = predCursor.value().intIterator()
      while (c.hasNext()) {
        val currentTrans = pred.transitions.get(c.next())
        currentTrans(n.childPos) = existing
      }
      val t = pred.successors(n.childPos).remove(n)
      if (pred.successors(n.childPos).containsKey(existing)) {
        pred.successors(n.childPos).get(existing).addAll(t)
        existing.predecessors.get(pred).addAll(t)
      } else {
        pred.successors(n.childPos).put(existing, t)
        existing.predecessors.put(pred, t)
      }
    }
    if (n.successors != null) {
      for (sucL <- n.successors) {
        val sucCursor = sucL.cursor()
        while (sucCursor.moveNext()) {
          sucCursor.key().predecessors.remove(n)
        }
      }
    }
    n.eraseData()
  }

  @SerialVersionUID(1L)
  class AutomatonNode(var codeVertexId: Int, var childPos: Int, var transitions: IntObjMap[ArrayBuffer[AutomatonNode]]) extends Serializable {
    var id: Int = getNextNodeId
    //    if (id > MAX_21_BITS) throw new RuntimeException("id too high, we only have 21 bits")
    transitions.shrink()
    var successors: IndexedSeq[ObjObjMap[AutomatonNode, IntArrayList]] = null
    var predecessors: ObjObjMap[AutomatonNode, IntArrayList] = null
    //    if(transitions == null) throw new RuntimeException("ho")

    //    def this(codeVertexId: Int, childPos: Int, m: IntObjMap[_], n: ArrayBuffer[AutomatonNode]) = {
    //      this(codeVertexId, childPos, HashIntObjMaps.getDefaultFactory[ArrayBuffer[AutomatonNode]].withHashConfig(AutomaTree.dsfsdfds).newMutableMap(m.size()))
    //      val cursor = m.cursor()
    //      while (cursor.moveNext()) {
    //        this.transitions.put(cursor.key(), n)
    //      }
    //    }

    def this(codeVertexId: Int, childPos: Int, s: IntSet, n: ArrayBuffer[AutomatonNode]) = {
      this(codeVertexId, childPos, new FakeMap(AutomaTree.instantiateIntSet(s), n))
    }

    def this(codeVertexId: Int, childPos: Int, m: Iterable[Int], n: ArrayBuffer[AutomatonNode]) = {
      this(codeVertexId, childPos, new FakeMap(AutomaTree.instantiateIntSet(m.size), n))
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
    }

    @throws(classOf[IOException])
    private def readObject(in: ObjectInputStream): Unit = {
      this.id = in.readInt() //Random.nextInt
      this.childPos = in.readInt()
      this.codeVertexId = in.readInt()
      val nbTrans = in.readInt()
      //      println("nb Trans " + nbTrans)
      if (nbTrans > 0) {
        if (in.readBoolean()) {
          //FIXME this only works with null leaves
          this.transitions = new FakeMap[ArrayBuffer[AutomatonNode]](AutomaTree.instantiateIntSet(nbTrans), null)
        } else {
          this.transitions = AutomaTree.instantiateIntObjectMap[ArrayBuffer[AutomatonNode]](nbTrans)
        }
        for (i <- 0 until nbTrans) {
          this.transitions.put(in.readInt(), in.readObject().asInstanceOf[ArrayBuffer[AutomatonNode]])
        }
        transitions.shrink()
      }
      //      println("object size " + Instru.getObjectSize(this))
      //      println("transitions size " + Instru.getObjectSize(this.transitions))
      //      println("nb trans " + transitions.size())
      //            println("deserialized with id " + this.id)
    }

    override def toString(): String = {
      "n" + this.id + " (" + this.codeVertexId + ")"
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

    def validateVertices(distinctTrans: IntSet, knownPathBufferIn1: IntIntMap, knownPathBufferIn2: IntIntMap, blockedPaths: IntIntMap, releasePaths: Array[IntSet], lockLevels: Array[TreeSet[Int]]) = {
      //      debug = false
      //      preDebug = false
      //      if (this.id == 1444) {
      //        //            if (id == 1065 && transitions.containsKey(2480)) {
      //        //        val iter = predecessors.cursor()
      //        //        iter.moveNext()
      //        //        if (iter.key.id == 586) {
      //        //          println(transitions.size() + "\t" + successors.size + "\t" + this.codeVertexId + "\t" + this.childPos)
      //        debug = true
      //        //        }
      //      }
      if (successors == null || successors.size == 1) {
        var knownPathBuffer1 = knownPathBufferIn1
        var knownPathBuffer2 = knownPathBufferIn2
        if (this.successors == null) {
          //last level
          val transitionsCursor = transitions.cursor()
          var knownPath: IntIntMap = null
          while (transitionsCursor.moveNext()) {
            //known path only usable if same successor node
            if (knownPath == null || knownPath.containsKey(transitionsCursor.key())) {
              val path = existsPath(transitionsCursor.key(), transitionsCursor.value(), this, knownPathBuffer1, blockedPaths, releasePaths, lockLevels)
              if (path == null) {
                transitionsCursor.remove()
              } else {
                if (parallelValidation) {
                  distinctTrans.synchronized {
                    distinctTrans.add(transitionsCursor.key())
                  }
                } else {
                  distinctTrans.add(transitionsCursor.key())
                }
                knownPath = path
                val swap = knownPathBuffer1
                knownPathBuffer1 = knownPathBuffer2
                knownPathBuffer2 = swap
              }
            } else {
              if (parallelValidation) {
                distinctTrans.synchronized {
                  distinctTrans.add(transitionsCursor.key())
                }
              } else {
                distinctTrans.add(transitionsCursor.key())
              }
            }
          }
        } else {
          val sucCursor = successors(0).cursor()
          while (sucCursor.moveNext()) {
            val successor = sucCursor.key()
            val vCur = sucCursor.value().intIterator()
            var knownPath: IntIntMap = null
            while (vCur.hasNext()) {
              val elem = vCur.next
              //              debug = false
              //              if (preDebug /*&& vCur.elem() == 2104508*/ ) {
              //                println("going in")
              //                debug = true
              //              }
              //known path only usable if same successor node
              if (knownPath == null || knownPath.containsKey(elem)) {
                val path = existsPath(elem, ArrayBuffer(successor), this, knownPathBuffer1, blockedPaths, releasePaths, lockLevels)
                if (path == null) {
                  this.transitions.remove(elem)
                  vCur.remove()
                } else {
                  if (parallelValidation) {
                    distinctTrans.synchronized {
                      distinctTrans.add(elem)
                    }
                  } else {
                    distinctTrans.add(elem)
                  }
                  knownPath = path
                  val swap = knownPathBuffer1
                  knownPathBuffer1 = knownPathBuffer2
                  knownPathBuffer2 = swap
                }
              } else {
                if (parallelValidation) {
                  distinctTrans.synchronized {
                    distinctTrans.add(elem)
                  }
                } else {
                  distinctTrans.add(elem)
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
          //          debug = false
          //          if (preDebug && transitionsCursor.key() == 2010081) {
          //            println("going in")
          //            debug = true
          //          }
          val sw = new StopWatch
          sw.start()
          val path = existsPath(transitionsCursor.key(), transitionsCursor.value(), this, knownPathBufferIn1, blockedPaths, releasePaths, lockLevels)
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
            if (parallelValidation) {
              distinctTrans.synchronized {
                distinctTrans.add(transitionsCursor.key())
              }
            } else {
              distinctTrans.add(transitionsCursor.key())
            }
          }
        }
      }
      if (this.transitions.isEmpty() && this != root) {
        throw new RuntimeException("impossible, parent checked " + this + " parents " + this.predecessors)
        this.eraseData()
      } else {
        this.transitions.shrink()
      }
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

    //TODO Don't store lists if size > depth cause we won t actually read them
    def prepSuccessorsAndPredecessors: Unit = {
      if (this != root) {
        this.predecessors = AutomaTree.instantiateObjectObjectMap[AutomatonNode, IntArrayList]
      }
      val transCursor = transitions.cursor()
      while (transCursor.moveNext()) {
        val nodes = transCursor.value()
        if (nodes == null) {
          return
        } else {
          for (i <- 0 until nodes.length) {
            if (this.successors == null) {
              this.successors = ArrayBuffer.fill(nodes.length)(AutomaTree.instantiateObjectObjectMap[AutomatonNode, IntArrayList])
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

    private def existsPath(v: Int, outTrans: ArrayBuffer[AutomatonNode], source: AutomatonNode, consumed: IntIntMap, blockedPaths: IntIntMap, releasePaths: Array[IntSet], lockLevels: Array[TreeSet[Int]]): IntIntMap = {
      consumed.clear()
      blockedPaths.clear()
      for (s <- releasePaths) {
        s.clear()
      }
      consumed.put(v, 0)
      //      debug = false
      //      if (id == 53848) {
      //        debug = true
      //      }
      //min added by reached, recursion added by
      def recExistsPath(toDoDown: List[(Int, AutomatonNode)], toDoUp: (Int, AutomatonNode), step: Int): (Int, Int) = {
        //        if (debug) println(step + "\t" + toDoDown + "\t" + toDoUp + "\t" + consumed + "\t" + blockedPaths)
        if (toDoDown.isEmpty && (toDoUp == null || toDoUp._2.predecessors == null)) return (-1, -1)
        lockLevels(step).clear()
        var nextStep: AutomatonNode = null
        var locksGoingTo: Int = -1
        var lowestBacktrackPoint = step
        if (toDoDown.isEmpty) {
          nextStep = toDoUp._2
          locksGoingTo = toDoUp._1
          //          if (debug) println(step + " trying to go up " + nextStep.predecessors.keySet())
          val suitablePredecessor = nextStep.predecessors.cursor()
          while (suitablePredecessor.moveNext()) {
            val bs = blockedPaths.get(suitablePredecessor.key.id)
            if (bs == -1) {
              //              if (debug) println(step + " trying to go up with " + suitablePredecessor.key() + " - " + suitablePredecessor.value)
              if (suitablePredecessor.key().successors.size == 1) {
                if (suitablePredecessor.value.size >= nbNodes) {
                  var nextToDoUp = suitablePredecessor.key()
                  val recRes = recExistsPath(toDoDown, (step, nextToDoUp), step + 1)
                  //                  if (debug) println(step + " rec res " + recRes)
                  if (recRes._1 == -1) {
                    return (-1, -1)
                  } else {
                    lowestBacktrackPoint = Math.min(recRes._1, lowestBacktrackPoint)
                    if (!releasePaths(step).isEmpty()) {
                      val c = releasePaths(step).cursor()
                      while (c.moveNext()) {
                        blockedPaths.remove(c.elem())
                        c.remove()
                      }
                    }
                  }
                } else {
                  val transIter = suitablePredecessor.value().intIterator()
                  var fastExit = false
                  while (transIter.hasNext() && !fastExit) {
                    val elem = transIter.next
                    val putRes = consumed.putIfAbsent(elem, step)
                    if (putRes == -1) {
                      var nextToDoUp = suitablePredecessor.key()
                      val recRes = recExistsPath(toDoDown, (step, nextToDoUp), step + 1)
                      //                      if (debug) println(step + " rec res " + recRes)
                      if (recRes._1 == -1) {
                        return (-1, -1)
                      } else {
                        lowestBacktrackPoint = Math.min(recRes._1, lowestBacktrackPoint)
                        if (!releasePaths(step).isEmpty()) {
                          val c = releasePaths(step).cursor()
                          while (c.moveNext()) {
                            blockedPaths.remove(c.elem())
                            c.remove()
                          }
                        }
                        if ((lockLevels(step + 1).isEmpty() || lockLevels(step + 1).last() < step)) fastExit = true
                      }
                      consumed.remove(elem)
                    } else {
                      lockLevels(step).add(putRes)
                    }
                  }
                }
              } else {
                val transIter = suitablePredecessor.value().intIterator()
                while (transIter.hasNext()) {
                  val elem = transIter.next
                  val putRes = consumed.putIfAbsent(elem, step)
                  if (putRes == -1) {
                    var nextToDoUp = suitablePredecessor.key()
                    var nextToDoDown: List[(Int, AutomatonNode)] = toDoDown
                    val successors = nextToDoUp.transitions.get(elem)
                    if (successors != null) {
                      for (i <- 0 until successors.size) {
                        if (i != nextStep.childPos) {
                          nextToDoDown = (step, successors(i)) :: nextToDoDown
                        }
                      }
                    }
                    val recRes = recExistsPath(nextToDoDown, (step, nextToDoUp), step + 1)
                    //                    if (debug) println(step + " rec res " + recRes)
                    if (recRes._1 == -1) {
                      return (-1, -1)
                    } else {
                      if (!releasePaths(step).isEmpty()) {
                        val c = releasePaths(step).cursor()
                        while (c.moveNext()) {
                          blockedPaths.remove(c.elem())
                          c.remove()
                        }
                      }
                      lowestBacktrackPoint = Math.min(recRes._1, lowestBacktrackPoint)
                    }
                    consumed.remove(elem)
                  } else {
                    lockLevels(step).add(putRes)
                  }
                }
              }
            } else {
              lockLevels(step).add(bs)
            }
          }
        } else {
          nextStep = toDoDown.head._2
          locksGoingTo = toDoDown.head._1
          //          if (debug) println(step + " trying to go down with " + nextStep + "\t" + nextStep.transitions.keySet())
          if (nextStep.successors == null) {
            if (nextStep.transitions.size >= nbNodes) {
              //              if (debug) println(step + " " + nextStep + " no successors and enough nodes")
              val recRes = recExistsPath(toDoDown.tail, toDoUp, step + 1)
              //              if (debug) println(step + " rec res " + recRes)
              if (recRes._1 == -1) {
                return (-1, -1)
              } else {
                lowestBacktrackPoint = Math.min(recRes._1, lowestBacktrackPoint)
              }
            } else {
              //              if (debug) println(step + " " + nextStep + " no successors")
              val nextTrans = nextStep.transitions.cursor()
              var fastExit = false
              while (nextTrans.moveNext() && !fastExit) {
                var plannedBlockLevel = -1
                val blockUp = blockedPaths.get(toDoUp._2.id)
                if (blockUp != -1) {
                  plannedBlockLevel = Math.max(blockUp, toDoUp._1)
                  lockLevels(step).add(blockUp)
                } else {
                  for (n <- toDoDown.tail) {
                    val blockDown = blockedPaths.get(n._2.id)
                    if (blockDown != -1) {
                      val level = Math.max(blockDown, n._1)
                      if (plannedBlockLevel == -1 || level < plannedBlockLevel) {
                        plannedBlockLevel = level
                      }
                      lockLevels(step).add(blockDown)
                    }
                  }
                }
                if (plannedBlockLevel != -1) {
                  //                  if (debug) println(step + " reseting locks (2) to " + plannedBlockLevel)
                  lockLevels(step).tailSet(plannedBlockLevel).clear()
                  lockLevels(step).add(plannedBlockLevel)
                  fastExit = true
                } else {
                  //                  if (debug) println(step + " trying " + nextStep + " with " + nextTrans.key())
                  val putRes = consumed.putIfAbsent(nextTrans.key(), step)
                  if (putRes == -1) {
                    val recRes = recExistsPath(toDoDown.tail, toDoUp, step + 1)
                    //                    if (debug) println(step + " rec res " + recRes)
                    if (recRes._1 == -1) {
                      return (-1, -1)
                    } else {
                      lowestBacktrackPoint = Math.min(recRes._1, lowestBacktrackPoint)
                      if ((lockLevels(step + 1).isEmpty() || lockLevels(step + 1).last() < step)) fastExit = true
                      if (!releasePaths(step).isEmpty()) {
                        val c = releasePaths(step).cursor()
                        while (c.moveNext()) {
                          blockedPaths.remove(c.elem())
                          c.remove()
                        }
                      }
                      consumed.remove(nextTrans.key())
                    }
                  } else {
                    lockLevels(step).add(putRes)
                  }
                }
              }
            }
          } else if (nextStep.successors.size == 1) {
            var superFastExit = false
            //            if (debug) println(step + " " + nextStep + " one successor")
            val sucCur = nextStep.successors(0).cursor()
            while (sucCur.moveNext() && !superFastExit) {
              val bs = blockedPaths.get(sucCur.key.id)
              if (bs == -1) {
                if (sucCur.value().size() >= nbNodes) {
                  //                  if (debug) println(step + " " + nextStep + " by " + sucCur.key() + " enough nodes")
                  val recRes = recExistsPath((step, sucCur.key()) :: toDoDown.tail, toDoUp, step + 1)
                  //                  if (debug) println(step + " rec res " + recRes)
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
                    val blockUp = blockedPaths.get(toDoUp._2.id)
                    if (blockUp != -1) {
                      plannedBlockLevel = Math.max(blockUp, toDoUp._1)
                      lockLevels(step).add(blockUp)
                    } else {
                      for (n <- toDoDown.tail) {
                        val blockDown = blockedPaths.get(n._2.id)
                        if (blockDown != -1) {
                          val level = Math.max(blockDown, n._1)
                          if (plannedBlockLevel == -1 || level < plannedBlockLevel) {
                            plannedBlockLevel = level
                          }
                          lockLevels(step).add(blockDown)
                        }
                      }
                    }
                    if (plannedBlockLevel != -1) {
                      //                      if (debug) println(step + " reseting locks (1) to " + plannedBlockLevel)
                      lockLevels(step).tailSet(plannedBlockLevel).clear()
                      lockLevels(step).add(plannedBlockLevel)
                      superFastExit = true
                      fastExit = true
                    } else {
                      //                      if (debug) println(step + " trying " + nextStep + " by " + sucCur.key() + " with " + nextTrans.elem())
                      val putRes = consumed.putIfAbsent(elem, step)
                      if (putRes == -1) {
                        val recRes = recExistsPath((step, sucCur.key()) :: toDoDown.tail, toDoUp, step + 1)
                        //                        if (debug) println(step + " rec res " + recRes)
                        if (recRes._1 == -1) {
                          return (-1, -1)
                        } else {
                          lowestBacktrackPoint = Math.min(recRes._1, lowestBacktrackPoint)
                          if ((lockLevels(step + 1).isEmpty() || lockLevels(step + 1).last() < step)) fastExit = true
                          if (!releasePaths(step).isEmpty()) {
                            val c = releasePaths(step).cursor()
                            while (c.moveNext()) {
                              blockedPaths.remove(c.elem())
                              c.remove()
                            }
                          }
                        }
                        consumed.remove(elem)
                      } else {
                        lockLevels(step).add(putRes)
                      }
                    }
                  }
                }
              } else {
                lockLevels(step).add(bs)
              }
            }
          } else {
            //            if (debug) println(step + " " + nextStep + " >1 successor")
            val nextTrans = nextStep.transitions.cursor()
            var fastExit = false
            while (nextTrans.moveNext() && !fastExit) {
              var plannedBlockLevel = -1
              val blockUp = blockedPaths.get(toDoUp._2.id)
              if (blockUp != -1) {
                plannedBlockLevel = Math.max(blockUp, toDoUp._1)
                lockLevels(step).add(blockUp)
              } else {
                for (n <- toDoDown.tail) {
                  val blockDown = blockedPaths.get(n._2.id)
                  if (blockDown != -1) {
                    val level = Math.max(blockDown, n._1)
                    if (plannedBlockLevel == -1 || level < plannedBlockLevel) {
                      plannedBlockLevel = level
                    }
                    lockLevels(step).add(blockDown)
                  }
                }
              }
              if (plannedBlockLevel != -1) {
                //                if (debug) println(step + " reseting locks (2) to " + plannedBlockLevel)
                lockLevels(step).tailSet(plannedBlockLevel).clear()
                lockLevels(step).add(plannedBlockLevel)
                fastExit = true
              } else {
                //                if (debug) println(step + " trying " + nextStep + " by " + nextTrans.value() + " with " + nextTrans.key())
                val putRes = consumed.putIfAbsent(nextTrans.key(), step)
                if (putRes == -1) {
                  val additionalToDoDown = nextTrans.value().toList.map(p => (step, p))
                  var bs = -1
                  for (n <- additionalToDoDown) {
                    val l = blockedPaths.get(n._2.id)
                    if (l != -1 && (bs == -1 || l < bs)) {
                      bs = l
                    }
                  }
                  if (bs == -1) {
                    val recRes = recExistsPath(additionalToDoDown ++ toDoDown.tail, toDoUp, step + 1)
                    //                    if (debug) println(step + " rec res " + recRes)
                    if (recRes._1 == -1) {
                      return (-1, -1)
                    } else {
                      lowestBacktrackPoint = Math.min(recRes._1, lowestBacktrackPoint)
                      if (!releasePaths(step).isEmpty()) {
                        val c = releasePaths(step).cursor()
                        while (c.moveNext()) {
                          blockedPaths.remove(c.elem())
                          c.remove()
                        }
                      }
                    }
                  } else {
                    lockLevels(step).add(bs)
                  }
                  consumed.remove(nextTrans.key())
                } else {
                  lockLevels(step).add(putRes)
                }
              }
            }
          }
        }
        if (locksGoingTo != step - 1) {
          //          if (debug) println(step + " transfering " + lockLevels(step).tailSet(locksGoingTo).headSet(step - 1) + " to " + (step - 1))
          lockLevels(step - 1).addAll(lockLevels(step).tailSet(locksGoingTo).headSet(step - 1))
          //          if (debug) println(step + " transfering " + locksGoingTo + " to " + (step - 1))
          lockLevels(step - 1).add(locksGoingTo)
        }
        //        if (debug) println(step + " transfering " + lockLevels(step).headSet(locksGoingTo) + " to " + locksGoingTo)
        lockLevels(locksGoingTo).addAll(lockLevels(step).headSet(locksGoingTo))
        if (lowestBacktrackPoint == step || lowestBacktrackPoint == locksGoingTo) {
          if (!lockLevels(step).isEmpty()) {
            val lockingTo = lockLevels(step).last()
            //            if (debug) println(step + " locking " + nextStep + " to " + lockingTo)
            if (lockingTo >= step) throw new RuntimeException("not possible " + step + "-" + lockLevels(step))
            if (lockingTo < step - 1) {
              blockedPaths.put(nextStep.id, lockingTo)
              releasePaths(lockingTo).add(nextStep.id)
            }
          } else {
            //lock forever
            //            if (debug) println(step + " locking " + nextStep + " forever")
            blockedPaths.put(nextStep.id, 0)
          }
          lowestBacktrackPoint = locksGoingTo
        }
        //        if (debug) println("end of exec for " + nextStep + " lowestBacktrackPoint " + lowestBacktrackPoint + " levels are " + lockLevels(step))
        return (lowestBacktrackPoint, locksGoingTo)
      }

      var toDoUp = source
      var toDoDown: List[AutomatonNode] = if (outTrans != null) {
        outTrans.toList
      } else {
        Nil
      }
      val res = recExistsPath(toDoDown.map(t => (0, t)), (0, toDoUp), 1)
      //      if (source.codeVertexId == 0 && debug && v == 2960 || source.codeVertexId == 1 && debug && v == 2840 || source.codeVertexId == 2 && debug && v == 3281 || source.codeVertexId == 3 && debug && v == 3306) {
      //        println("found: " + v + "->" + res._1 + " for id " + source.id)
      //      }
      if (res._1 == -1) {
        //      if (res._1 == -1) {
        if (v == 2088061 && this.codeVertexId == 4) println(consumed)
        return consumed
      } else {
        return null
      }
    }
  }
}

object AutomaTree {
  //  val mapsConfig = HashConfig.def
  val intintMapFacto: HashIntIntMapFactory = HashIntIntMaps.getDefaultFactory.withDefaultValue(-1)
  //  val intSetIntMapFacto: HashObjIntMapFactory[IntSet] = HashObjIntMaps.getDefaultFactory[IntSet].withDefaultValue(-1)
  //  val MAX_21_BITS = 2000000
  //build dag options
  //  var collectTransitionsPerLevel = true
  //validate dag options
  //  var eliminateTransitionsEarly = true
  //  var forceFullTransitionCleanup = true
  //debug
  //  var debug = false

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

  def intersect(resAutomaton: AutomaTree)(n1: AutomaTree#AutomatonNode, n2: AutomaTree#AutomatonNode, interType: IntersectType, threshold: Int, doubleFFsource: Int): resAutomaton.AutomatonNode = {
    resAutomaton.transByLevel = Array.fill(resAutomaton.nbNodes)(AutomaTree.instantiateIntSet)
    val r = intersectDfstrat(resAutomaton)(n1, n2, interType, HashLongObjMaps.newMutableMap(), resAutomaton.transByLevel, null, doubleFFsource, Array.fill(resAutomaton.nbNodes)(new UnifiedMap))
    for (l <- resAutomaton.transByLevel) {
      if (l.size() < threshold) {
        return null
      }
    }
    return r
  }

  def extend(resAutomaton: AutomaTree)(n: AutomaTree#AutomatonNode, extensions: IntObjMap[_ <: ArrayBuffer[_ <: AutomaTree#AutomatonNode]], threshold: Int): resAutomaton.AutomatonNode = {
    resAutomaton.transByLevel = Array.fill(resAutomaton.nbNodes)(AutomaTree.instantiateIntSet)
    val d1Buff: IntObjMap[resAutomaton.AutomatonNode] = AutomaTree.instantiateIntObjectMap
    val leavesBuff: IntObjMap[resAutomaton.AutomatonNode] = AutomaTree.instantiateIntObjectMap
    val r = extendImp(resAutomaton)(n, extensions, d1Buff, leavesBuff, resAutomaton.transByLevel, Array.fill(resAutomaton.nbNodes)(new UnifiedMap))
    for (l <- resAutomaton.transByLevel) {
      if (l.size() < threshold) {
        return null
      }
    }
    return r
  }

  def project(resAutomaton: AutomaTree)(n: AutomaTree#AutomatonNode, filter: Array[IntSet], threshold: Int): resAutomaton.AutomatonNode = {
    val d1Buff: IntObjMap[resAutomaton.AutomatonNode] = AutomaTree.instantiateIntObjectMap
    var transByLevel: Array[IntSet] = Array.fill(resAutomaton.nbNodes)(AutomaTree.instantiateIntSet)
    val r = projectImp(resAutomaton)(n, filter, d1Buff, transByLevel, Array.fill(resAutomaton.nbNodes)(new UnifiedMap))
    for (l <- transByLevel) {
      if (l.size() < threshold) {
        return null
      }
    }
    return r
  }

  private def intersectDfstrat(resAutomaton: AutomaTree)(n1: AutomaTree#AutomatonNode, n2: AutomaTree#AutomatonNode, interType: IntersectType, nodeBuffer: LongObjMap[resAutomaton.AutomatonNode], transitionsPerLevel: Array[IntSet], extraTransitionsFromN2: AutomaTree#AutomatonNode, doubleFFsource: Int, revuz: Array[UnifiedMap[IntObjMap[ArrayBuffer[resAutomaton.AutomatonNode]], resAutomaton.AutomatonNode]]): resAutomaton.AutomatonNode = {
    //    if (n1.codeVertexId != n2.codeVertexId) {
    //      throw new RuntimeException("doing something incoherent")
    //    }
    //    println(n1  + " " + n1.codeVertexId +"\t" + n2 + "\t" + extraTransitionsFromN2)
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
    var transitions: IntObjMap[ArrayBuffer[resAutomaton.AutomatonNode]] = null
    if (n2 == null) {
      if (extraTransitionsFromN2 != null && n1.codeVertexId == extraTransitionsFromN2.codeVertexId) {
        val (small, large) = if (n1.transitions.size() < extraTransitionsFromN2.transitions.size()) {
          (n1, extraTransitionsFromN2)
        } else {
          (extraTransitionsFromN2, n1)
        }
        val cursor = small.transitions.cursor()
        while (cursor.moveNext()) {
          if (large.transitions.containsKey(cursor.key)) {
            val cl = large.transitions.get(cursor.key())
            val cs = cursor.value()
            if (transitions == null) {
              transitions = new FakeMap(AutomaTree.instantiateIntSet, null)
            }
            transitions.put(cursor.key(), null)
          }
        }
      } else {
        val cursor = n1.transitions.cursor()
        while (cursor.moveNext()) {
          if (cursor.value() == null) {
            if (transitions == null) {
              transitions = new FakeMap(AutomaTree.instantiateIntSet, null)
            }
            transitions.put(cursor.key(), null)
          } else {
            val c1 = cursor.value()
            var createdSuc: ArrayBuffer[resAutomaton.AutomatonNode] = null
            var allSucceeded = true
            for (i <- c1.size - 1 to 0 by -1 if allSucceeded) {
              val childrenInter = intersectDfstrat(resAutomaton)(c1(i), null, interType, nodeBuffer, transitionsPerLevel, if (i == c1.size - 1) extraTransitionsFromN2 else null, doubleFFsource, revuz)
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
                transitions = AutomaTree.instantiateIntObjectMap[ArrayBuffer[resAutomaton.AutomatonNode]]
              }
              transitions.put(cursor.key(), createdSuc)
            }
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
              transitions = new FakeMap(AutomaTree.instantiateIntSet, null)
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
                  //optim when fftoff and d1 and d2 are the same
                  if (c1 eq c2) {
                    resAutomaton.newNode(c1(i).codeVertexId, i, createdSuc(i + 1).transitions)
                  } else {
                    resAutomaton.newNode(c1(i).codeVertexId, i, c1(i).transitions, null)
                  }
                }
                createdSuc(i) = extraTrans
              } else {
                val childrenInter = intersectDfstrat(resAutomaton)(c1(i), c2(i), interType, nodeBuffer, transitionsPerLevel, extraTransitionsFromN2, doubleFFsource, revuz)
                if (childrenInter != null) {
                  createdSuc(i) = childrenInter
                } else {
                  allSucceeded = false
                }
              }
            }
            if (allSucceeded) {
              if (transitions == null) {
                transitions = AutomaTree.instantiateIntObjectMap[ArrayBuffer[resAutomaton.AutomatonNode]]
              }
              if (!firstExtraFromCache) {
                //TODO revuz shouldn t be necessary here if we trust that there are no dups in initial dags
                //                val existing = revuz(createdSuc(createdSuc.size - 1).codeVertexId).get(createdSuc(createdSuc.size - 1).transitions)
                //                if (existing == null) {
                //                  revuz(createdSuc(createdSuc.size - 1).codeVertexId).put(createdSuc(createdSuc.size - 1).transitions, createdSuc(createdSuc.size - 1))
                resAutomaton.nodesByDepth(createdSuc(createdSuc.size - 1).codeVertexId) += createdSuc(createdSuc.size - 1)
                transitionsPerLevel(createdSuc(createdSuc.size - 1).codeVertexId).addAll(createdSuc(createdSuc.size - 1).transitions.keySet())
                //                } else {
                //                  createdSuc(createdSuc.size - 1) = existing
                //                }
                nodeBuffer.put(extraKey1, createdSuc(createdSuc.size - 1))
              }
              if (!secondExtraFromCache) {
                //TODO revuz shouldn t be necessary here if we trust that there are no dups in initial dags
                //                val existing = revuz(createdSuc(createdSuc.size - 2).codeVertexId).get(createdSuc(createdSuc.size - 2).transitions)
                //                if (existing == null) {
                //                  revuz(createdSuc(createdSuc.size - 2).codeVertexId).put(createdSuc(createdSuc.size - 2).transitions, createdSuc(createdSuc.size - 2))
                resAutomaton.nodesByDepth(createdSuc(createdSuc.size - 2).codeVertexId) += createdSuc(createdSuc.size - 2)
                transitionsPerLevel(createdSuc(createdSuc.size - 2).codeVertexId).addAll(createdSuc(createdSuc.size - 2).transitions.keySet())
                //                } else {
                //                  createdSuc(createdSuc.size - 2) = existing
                //                }
                nodeBuffer.put(extraKey2, createdSuc(createdSuc.size - 2))
              }
              transitions.put(cursor.key(), createdSuc)
            }
          } else if (c1 != null && c2 != null && c1.size == c2.size) {
            //standard case + BBtoBB
            var createdSuc: ArrayBuffer[resAutomaton.AutomatonNode] = null
            var allSucceeded = true
            for (i <- c1.size - 1 to 0 by -1 if allSucceeded) {
              val childrenInter = intersectDfstrat(resAutomaton)(c1(i), c2(i), interType, nodeBuffer, transitionsPerLevel, if (i == c1.size - 1) extraTransitionsFromN2 else null, doubleFFsource, revuz)
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
                transitions = AutomaTree.instantiateIntObjectMap[ArrayBuffer[resAutomaton.AutomatonNode]]
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
                  val childrenInter = intersectDfstrat(resAutomaton)(c1(i), if (i == c1.size - 1) null else c2(i), interType, nodeBuffer, transitionsPerLevel, if (i == c1.size - 1) c2(c2.size - 1) else null, doubleFFsource, revuz)
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
                    transitions = AutomaTree.instantiateIntObjectMap[ArrayBuffer[resAutomaton.AutomatonNode]]
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
                    val childrenInter = intersectDfstrat(resAutomaton)(c1(i), c2(i), interType, nodeBuffer, transitionsPerLevel, extraTransitionsFromN2, doubleFFsource, revuz)
                    if (childrenInter != null) {
                      createdSuc(i) = childrenInter
                    } else {
                      allSucceeded = false
                    }
                  }
                }
                if (allSucceeded) {
                  if (!extraFromCache) {
                    //                    val existing = revuz(createdSuc(createdSuc.size - 1).codeVertexId).get(createdSuc(createdSuc.size - 1).transitions)
                    //                    if (existing == null) {
                    //TODO revuz shouldn t be necessary here if we trust that d2 has no dups
                    //                      revuz(createdSuc(createdSuc.size - 1).codeVertexId).put(createdSuc(createdSuc.size - 1).transitions, createdSuc(createdSuc.size - 1))
                    resAutomaton.nodesByDepth(createdSuc(createdSuc.size - 1).codeVertexId) += createdSuc(createdSuc.size - 1)
                    transitionsPerLevel(createdSuc(createdSuc.size - 1).codeVertexId).addAll(createdSuc(createdSuc.size - 1).transitions.keySet())
                    //                    } else {
                    //                      createdSuc(createdSuc.size - 1) = existing
                    //                    }
                    nodeBuffer.put(extraKey, createdSuc(createdSuc.size - 1))
                  }
                  if (transitions == null) {
                    transitions = AutomaTree.instantiateIntObjectMap[ArrayBuffer[resAutomaton.AutomatonNode]]
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
                    val childrenInter = intersectDfstrat(resAutomaton)(c1(i), c2(i), interType, nodeBuffer, transitionsPerLevel, extraTransitionsFromN2, doubleFFsource, revuz)
                    if (childrenInter != null) {
                      createdSuc(i) = childrenInter
                    } else {
                      allSucceeded = false
                    }
                  }
                }
                if (allSucceeded) {
                  if (!extraFromCache) {
                    val existing = revuz(createdSuc(createdSuc.size - 1).codeVertexId).get(createdSuc(createdSuc.size - 1).transitions)
                    if (existing == null) {
                      revuz(createdSuc(createdSuc.size - 1).codeVertexId).put(createdSuc(createdSuc.size - 1).transitions, createdSuc(createdSuc.size - 1))
                      resAutomaton.nodesByDepth(createdSuc(createdSuc.size - 1).codeVertexId) += createdSuc(createdSuc.size - 1)
                      transitionsPerLevel(createdSuc(createdSuc.size - 1).codeVertexId).addAll(createdSuc(createdSuc.size - 1).transitions.keySet())
                    } else {
                      createdSuc(createdSuc.size - 1) = existing
                    }
                    nodeBuffer.put(extraKey, createdSuc(createdSuc.size - 1))
                  }
                  if (transitions == null) {
                    transitions = AutomaTree.instantiateIntObjectMap[ArrayBuffer[resAutomaton.AutomatonNode]]
                  }
                  transitions.put(cursor.key(), createdSuc)
                }
              }
            }
          }
        }
      }
    }
    if (transitions == null) {
      nodeBuffer.put(key, null)
      //      if (nodeBuffer.size % 100000 == 0) println(nodeBuffer.size)
      return null
    } else {
      val existing = revuz(n1.codeVertexId).get(transitions)
      val intersectionRes = if (existing == null) {
        val n = resAutomaton.newNode(n1.codeVertexId, n1.childPos, transitions)
        revuz(n.codeVertexId).put(transitions, n)
        resAutomaton.nodesByDepth(n.codeVertexId) += n
        //        if (resAutomaton.nodesByDepth(n.codeVertexId).size % 10000 == 0) {
        //          println(resAutomaton.nodesByDepth.map(_.size).mkString(" - "))
        //        }
        transitionsPerLevel(n1.codeVertexId).addAll(n.transitions.keySet())
        n
      } else {
        existing
      }
      nodeBuffer.put(key, intersectionRes)
      //      if (nodeBuffer.size % 100000 == 0) println(nodeBuffer.size)
      return intersectionRes
    }
  }

  private def extendImp(resAutomaton: AutomaTree)(n: AutomaTree#AutomatonNode, extensions: IntObjMap[_ <: ArrayBuffer[_ <: AutomaTree#AutomatonNode]], thisBuffer: IntObjMap[resAutomaton.AutomatonNode], leavesBuffer: IntObjMap[resAutomaton.AutomatonNode], transitionsPerLevel: Array[IntSet], revuz: Array[UnifiedMap[IntObjMap[ArrayBuffer[resAutomaton.AutomatonNode]], resAutomaton.AutomatonNode]]): resAutomaton.AutomatonNode = {
    if (thisBuffer.containsKey(n.id)) {
      return thisBuffer.get(n.id)
    }
    var transitions: IntObjMap[ArrayBuffer[resAutomaton.AutomatonNode]] = null
    if (n.codeVertexId != resAutomaton.nbNodes - 2) {
      val cursor = n.transitions.cursor()
      while (cursor.moveNext()) {
        val successors = cursor.value()
        if (successors != null) {
          var createdSuc: ArrayBuffer[resAutomaton.AutomatonNode] = null
          var allSucceeded = true
          for (i <- 0 until successors.size if allSucceeded) {
            val rec = extendImp(resAutomaton)(successors(i), extensions, thisBuffer, leavesBuffer, transitionsPerLevel, revuz)
            if (rec == null) {
              allSucceeded = false
            } else {
              if (createdSuc == null) createdSuc = new ArrayBuffer(successors.size)
              createdSuc += rec
            }
          }
          if (allSucceeded) {
            if (transitions == null) {
              transitions = AutomaTree.instantiateIntObjectMap[ArrayBuffer[resAutomaton.AutomatonNode]](n.transitions.size())
            }
            transitions.put(cursor.key(), createdSuc)
          }
        } else {
          if (transitions == null) {
            transitions = new FakeMap(AutomaTree.instantiateIntSet(n.transitions.size()), null)
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
            resAutomaton.nodesByDepth(resAutomaton.nbNodes - 1) += leafNode
            transitionsPerLevel(resAutomaton.nbNodes - 1).addAll(leaf(0).transitions.keySet())
          }
          if (transitions == null) {
            transitions = AutomaTree.instantiateIntObjectMap[ArrayBuffer[resAutomaton.AutomatonNode]](n.transitions.size())
          }
          val ab = new ArrayBuffer[resAutomaton.AutomatonNode](1)
          ab += leafNode
          transitions.put(cursor.key, ab)
        }

      }
    }
    if (transitions != null) {
      val existing = revuz(n.codeVertexId).get(transitions)
      val extensionRes = if (existing == null) {
        val e = resAutomaton.newNode(n.codeVertexId, n.childPos, transitions)
        revuz(n.codeVertexId).put(transitions, e)
        resAutomaton.nodesByDepth(n.codeVertexId) += (e)
        transitionsPerLevel(n.codeVertexId).addAll(transitions.keySet())
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

  //  var recCounter = 0
  private def projectImp(resAutomaton: AutomaTree)(n: AutomaTree#AutomatonNode, filter: Array[IntSet], thisBuffer: IntObjMap[resAutomaton.AutomatonNode], transitionsPerLevel: Array[IntSet], revuz: Array[UnifiedMap[IntObjMap[ArrayBuffer[resAutomaton.AutomatonNode]], resAutomaton.AutomatonNode]]): resAutomaton.AutomatonNode = {
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
            val rec = projectImp(resAutomaton)(successors(i), filter, thisBuffer, transitionsPerLevel, revuz)
            if (rec == null) {
              allSucceeded = false
            } else {
              if (createdSuc == null) createdSuc = new ArrayBuffer(successors.size)
              createdSuc += rec
            }
          }
          if (allSucceeded) {
            if (transitions == null) {
              transitions = AutomaTree.instantiateIntObjectMap[ArrayBuffer[resAutomaton.AutomatonNode]](n.transitions.size())
            }
            transitions.put(cursor.key(), createdSuc)
          }
        } else {
          if (transitions == null) {
            transitions = new FakeMap(AutomaTree.instantiateIntSet(n.transitions.size()), null)
          }
          transitions.put(cursor.key(), null)
        }
      }
    }
    if (transitions != null) {
      val existing = revuz(n.codeVertexId).get(transitions)
      val extensionRes = if (existing == null) {
        val e = resAutomaton.newNode(n.codeVertexId, n.childPos, transitions)
        revuz(n.codeVertexId).put(transitions, e)
        transitionsPerLevel(n.codeVertexId).addAll(transitions.keySet())
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

  def groupExtendedTransitions(resAutomaton: AutomaTree)(extensions: IntObjMap[IntSet]): IntObjMap[ArrayBuffer[resAutomaton.AutomatonNode]] = {
    val res: IntObjMap[ArrayBuffer[resAutomaton.AutomatonNode]] = HashIntObjMaps.newMutableMap(extensions.size)
    val byNbSuccessors: IntObjMap[IntArrayList] = AutomaTree.instantiateIntObjectMap
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
    //    val d1: AutomaTree = new AutomaTree(List((1, 3), (1, 4), (2, 3), (2, 5)), false)
    //    val d2: AutomaTree = new AutomaTree(List((1, 3), (2, 3)), false)
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
