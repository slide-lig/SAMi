package fr.liglab.sami

import java.net.InetAddress
import java.rmi.RemoteException
import java.rmi.registry.LocateRegistry
import java.rmi.registry.Registry
import java.rmi.server.UnicastRemoteObject
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentSkipListSet

import scala.collection.mutable.ListBuffer

import fr.liglab.sami.code.DAG
import fr.liglab.sami.code.DFSCode
import fr.liglab.sami.code.DagProvider
import fr.liglab.sami.code.Edge
import fr.liglab.sami.code.FrequentGraph
import fr.liglab.sami.code.IntersectDag
import fr.liglab.sami.code.IntersectType
import fr.liglab.sami.code.IntersectType.IntersectType
import fr.liglab.sami.code.JoinDag
import fr.liglab.sami.utilities.StopWatch
import fr.liglab.sami.utilities.Marshal
import fr.liglab.sami.code.SeedDag
import scala.collection.immutable.HashMap
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.Queue

import java.nio.file._;
import java.io._

import scala.collection.JavaConverters._
import fr.liglab.sami.code.AutomaTree
import scala.collection.mutable.ArrayBuffer
import java.util.concurrent.atomic.AtomicInteger
import fr.liglab.sami.data.GraphInfo

class Workpool(val minSup: Int, maxSize: Int, val inputGraph: GraphInfo, nbThreads: Int, nodeMap: Map[String, Int], val dispEvery: Int) {
  var didAdvertise = false
  val basicTasks: Queue[BasicTask] = new ConcurrentLinkedQueue()
  val prefixTasks: ConcurrentSkipListSet[PrefixTask] = new ConcurrentSkipListSet()
  val delayedTasks: ConcurrentSkipListSet[BasicTask] = new ConcurrentSkipListSet()
  val tasksBeingStolen: ConcurrentHashMap[DFSCode, PrefixTask] = new ConcurrentHashMap()
  val dagsCachedByRemoteExecutors: ConcurrentHashMap[String, java.util.Set[Edge]] = new ConcurrentHashMap()
  @volatile var waitingThreads = 0
  @volatile var terminateThreads = false
  var frequentPatterns: ArrayBuffer[FrequentGraph] = ArrayBuffer.empty
  val localNodeName = InetAddress.getLocalHost().getHostName()
  val localNodeAddr = InetAddress.getLocalHost().getHostAddress()
  var localRegistry: Registry = null
  @volatile var neighbors = nodeMap.keys.filter(_ != localNodeAddr).toList
  @volatile var wantToShutDown = false
  val remoteWorkpools: ConcurrentHashMap[String, RemoteWorkpool] = new ConcurrentHashMap()
  var currentlyStealing: (RemoteWorkpool, DFSCode, java.util.Map[Edge, DagProvider]) = null

  def getPatterns(): List[FrequentGraph] = frequentPatterns.toList

  def addPattern(p: FrequentGraph) = {
    //    println("collecting " + p)
    val s = this.frequentPatterns.synchronized {
      this.frequentPatterns += p
      this.frequentPatterns.size
    }
    if (s % dispEvery == 0) {
      println("patterns found so far: " + this.frequentPatterns.size)
    }
  }

  def init(seeds: List[Edge], numPartitions: Int) = {
    val emptyPrefix = new DFSCode(Vector(), 0)
    val partitionIndex = nodeMap(localNodeAddr)
    var localSeeds = if (numPartitions > 1) seeds.zipWithIndex.filter(e => SamiRunner.smarterRoundRobin(e._2, partitionIndex, numPartitions)).map(_._1) else seeds
    //    localSeeds = localSeeds.filter(e => e.iLabel == 16 && e.jLabel == 20)
    println("nb localseeds: " + localSeeds.size)

    //debug: print prefix task list
    writeWorkFile()

    prefixTasks.add(new PrefixTask(this, emptyPrefix, localSeeds, inputGraph))
    for (s <- localSeeds) {
      this.addPattern(new FrequentGraph(emptyPrefix.extend(s), inputGraph.getExtensions(s).MNISupport))
    }
    if (neighbors.size > 0)
      this.advertiseWork()
  }

  def consumeDelayedTask: BasicTask = {
    if (!this.delayedTasks.isEmpty) {
      val iter = delayedTasks.iterator()
      while (iter.hasNext()) {
        val t = iter.next()
        if (t.tryLock) {
          //necessary because iter.remove doesn't return true/false
          if (delayedTasks.remove(t)) {
            this.synchronized {
              if (waitingThreads > 0) {
                waitingThreads = 0
                this.notifyAll()
              }
            }
            return t
          }
        }
      }
    }
    return null
  }

  def getLocalWork(): BasicTask = {
    //    println(localNodeAddr + "[" + Thread.currentThread().getName + " getLocalWork")
    while (true) {
      val t = consumeDelayedTask
      if (t != null) return t
      var basicTask = basicTasks.poll
      if (basicTask != null) {
        if (basicTask.tryLock) {
          return basicTask
        } else {
          delayedTasks.add(basicTask)
        }
      } else {
        this.synchronized {
          basicTask = basicTasks.poll
          if (basicTask != null) {
            if (basicTask.tryLock) {
              return basicTask
            } else {
              delayedTasks.add(basicTask)
            }
          } else {
            while (this.genMoreBasicTasks()) {
              wantToShutDown = false
              if (waitingThreads > 0) {
                waitingThreads = 0
                this.notifyAll()
              }
              basicTask = basicTasks.poll
              if (basicTask != null) {
                if (basicTask.tryLock) {
                  return basicTask
                } else {
                  delayedTasks.add(basicTask)
                }
              }
            }
            if (waitingThreads == nbThreads - 1) {
              val lastAttempt = consumeDelayedTask
              if (lastAttempt != null) lastAttempt
              if (!this.delayedTasks.isEmpty()) throw new RuntimeException("shutting down with delayed tasks to do")
              println(Thread.currentThread().getName + "attempt shutdown")
              wantToShutDown = true
              if (attemptShutdown) {
                println(Thread.currentThread().getName + "shutdown accepted")
                if (didAdvertise) {
                  try {
                    localRegistry.unbind("RemoteWorkpool");
                  } catch {
                    case ce: Exception => {
                      println("caught exception unbiding");
                      ce.printStackTrace()
                    }
                  }
                }
                terminateThreads = true
                this.frequentPatterns.synchronized {
                  this.frequentPatterns.notify()
                }
                this.notifyAll()
                return null
              } else {
                println(Thread.currentThread().getName + "shutdown denied")
                //sleeping for 5 seconds
                Thread.sleep(5000)
              }
            } else {
              waitingThreads += 1
              //println(Thread.currentThread().getName + " going to sleep, " + waitingThreads + " threads sleeping")
              this.wait()
              if (terminateThreads) {
                return null
              }
              //println(Thread.currentThread().getName + " waking up")
            }
          }
        }
      }
    }
    return null
  }

  def attemptShutdown(): Boolean = {
    println("attempt shutdown")
    if (!didAdvertise) return true
    var sw = new StopWatch()
    sw.start()
    if (neighbors.size == 0) return false
    neighbors.par.foreach { n =>

      if (!remoteWorkpools.containsKey(n)) {
        try {
          val registry: Registry = LocateRegistry.getRegistry(n)
          remoteWorkpools.put(n, registry.lookup("RemoteWorkpool").asInstanceOf[RemoteWorkpool])
        } catch {
          case ce: Exception => {
            println("caught exception when connecting");
            ce.printStackTrace()
            remoteWorkpools.remove(n)
          }
        }
      }
    }
    println(localNodeAddr + "[" + Thread.currentThread.getName() + "] connect time spent: " + sw.getElapsedTime())
    sw.start()

    val canShutdown = neighbors.par.forall { n =>
      if (remoteWorkpools.containsKey(n)) {
        val stub = remoteWorkpools.get(n)
        try {
          stub.askShutdown()
        } catch {
          case ce: Exception => {
            println("caught exception when askShutdown");
            ce.printStackTrace()
            remoteWorkpools.remove(n)
            true
          }
        }
      } else {
        true
      }
    }
    println(localNodeAddr + "[" + Thread.currentThread.getName() + "] attempt shutdown time spent: " + sw.getElapsedTime() + " answer is " + canShutdown)
    return canShutdown
  }

  def advertiseWork() = {
    try {
      val stub: RemoteWorkpool = UnicastRemoteObject.exportObject(new RemoteWorkpoolImpl(this), 0).asInstanceOf[RemoteWorkpool]
      localRegistry = LocateRegistry.getRegistry(localNodeAddr)
      localRegistry.bind("RemoteWorkpool", stub);
      println("workpool " + localNodeAddr + " advertised");
      println(stub)
      println(localRegistry)
      didAdvertise = true
    } catch {
      case e: Exception => {
        println("Workpool advertiseWork exception: " + e.toString());
        e.printStackTrace();
      }
    }
  }

  def sortDFSCodePriority(c1: DFSCode, c2: DFSCode): Boolean = {
    val code1 = c1
    val code2 = c2
    if (code1.codeLength() < code2.codeLength())
      return true
    if (code1.codeLength() > code2.codeLength())
      return false
    return code1 < code2
  }
  def stealWork(): Boolean = {
    try {
      if (currentlyStealing != null) {
        var sw = new StopWatch()
        sw.start()
        val s = currentlyStealing._1.stealMore(localNodeAddr, currentlyStealing._2)
        if (s != null) {
          if (s.prefix.codeLength() != 0) {
            val extIter = s.extensions.entrySet().iterator()
            while (extIter.hasNext()) {
              val p = extIter.next()
              currentlyStealing._3.put(p.getKey, p.getValue)
            }
            s.extensions = currentlyStealing._3
            if (s.d1 == null) {
              s.d1 = currentlyStealing._3.get(s.p1)
              if (s.d1 == null) throw new RuntimeException("I do not have a dag I'm supposed to have")
            } else {
              currentlyStealing._3.put(s.p1, s.d1)
            }
          }
          s.workpool = this
          if (s.genBasicTasks()) {

            println(localNodeAddr + "[" + Thread.currentThread.getName() + "] stealmore time spent: " + sw.getElapsedTime())
            return true
          } else {
            if (s.p1.isForwardEdge()) {
              println(localNodeAddr + "[" + Thread.currentThread.getName() + "] stealmore didn't create any basic task")
            } else {
              println(localNodeAddr + "[" + Thread.currentThread.getName() + "] <- " + currentlyStealing._1 + " " + s.prefix + " with " + s.p1 + " with extensions " + s.extensions)
              throw new RuntimeException("did stealmore and got nothing out of it")
            }
          }
        } else {
          currentlyStealing = null
          println(localNodeAddr + "[" + Thread.currentThread.getName() + "] stealmore gave null")
        }
      }
      var sw = new StopWatch()
      sw.start()
      if (neighbors.size == 0) return false
      neighbors.par.foreach { n =>

        if (!remoteWorkpools.containsKey(n)) {
          try {
            val registry: Registry = LocateRegistry.getRegistry(n)
            remoteWorkpools.put(n, registry.lookup("RemoteWorkpool").asInstanceOf[RemoteWorkpool])
          } catch {
            case ce: Exception => {
              println("caught exception when connecting");
              ce.printStackTrace()
              remoteWorkpools.remove(n)
            }
          }
        }
      }
      println(localNodeAddr + "[" + Thread.currentThread.getName() + "] connect time spent: " + sw.getElapsedTime())
      sw.start()
      val stealPriority = neighbors.par.map { n =>
        if (remoteWorkpools.containsKey(n)) {
          val stub = remoteWorkpools.get(n)
          var peekResult: DFSCode = null
          try {
            peekResult = stub.peekTask()
          } catch {
            case ce: Exception =>
              println("caught exception when peeking, removing " + n + " from remoteWorkpools list");
              // ce.printStackTrace()
              remoteWorkpools.remove(n)
          }
          (peekResult, (stub, n))
        } else {
          (null, null)
        }
      }.filter(_._1 != null).seq
        .sortWith { (x, y) => sortDFSCodePriority(x._1, y._1) }

      println(localNodeAddr + "[" + Thread.currentThread.getName() + "] peeking time spent: " + sw.getElapsedTime())

      for (c <- stealPriority) {
        val neighborToStealFrom = c._2
        sw = new StopWatch()
        sw.start()
        val s = neighborToStealFrom._1.stealTask(localNodeAddr)
        println(localNodeAddr + "[" + Thread.currentThread.getName() + "] stealing time spent: " + sw.getElapsedTime())
        if (s != null) {
          currentlyStealing = (neighborToStealFrom._1, s.prefix, new java.util.HashMap())
          if (s.prefix.codeLength() != 0) {
            val extIter = s.extensions.entrySet().iterator()
            while (extIter.hasNext()) {
              val p = extIter.next()
              currentlyStealing._3.put(p.getKey, p.getValue)
            }
            s.extensions = currentlyStealing._3
            if (s.d1 == null) {
              s.d1 = currentlyStealing._3.get(s.p1)
              if (s.d1 == null) throw new RuntimeException("I do not have a dag I'm supposed to have")
            } else {
              currentlyStealing._3.put(s.p1, s.d1)
            }
          }
          s.workpool = this
          if (s.genBasicTasks()) {
            println(localNodeAddr + "[" + Thread.currentThread.getName() + "] <- " + neighborToStealFrom._2 + " " + s.prefix + " with " + s.p1)
            return true
          } else {
            if (s.p1.isForwardEdge()) {
              println(localNodeAddr + "[" + Thread.currentThread.getName() + "] steal didn't create any basic task")
            } else {
              println(localNodeAddr + "[" + Thread.currentThread.getName() + "] <- " + neighborToStealFrom._2 + " " + s.prefix + " with " + s.p1 + " with extensions " + currentlyStealing._3.keySet())
              throw new RuntimeException("did steal and got nothing out of it")
            }
          }
        }
      }
    } catch {
      case e: Exception => {
        println("Workpool stealWork exception: " + e.toString());
        e.printStackTrace();
      }
    }
    return false
  }

  def genMoreBasicTasks(): Boolean = {
    while (true) {
      try {
        val pTask = prefixTasks.first()
        if (pTask.genBasicTasks) {
          return true
        } else {
          prefixTasks.remove(pTask)
        }
      } catch {
        case nse: NoSuchElementException => return stealWork
      }
    }
    return false
  }

  class BasicTask(private val collector: ResultsCollector, val t: IntersectType, d1: DagProvider, d2: DagProvider, val edgeExtension: Edge) extends Ordered[BasicTask] with Serializable {
    def tryLock: Boolean = d1.tryLock && d2.tryLock // ok because only one actually has a lock
    //    val execCounter: AtomicInteger = new AtomicInteger(0)
    override def toString: String = {
      return "BasicTask[" + collector.prefix + " " + t + " with " + edgeExtension + "]"
    }

    override def compare(that: BasicTask): Int = {
      if (this == that) return 0
      val prefixDiff = this.collector.prefix.compare(that.collector.prefix)
      if (prefixDiff != 0) return prefixDiff
      val edgeDiff = this.edgeExtension.compareTo(that.edgeExtension)
      if (edgeDiff != 0) return edgeDiff
      val typeDiff = this.t.compare(that.t)
      if (typeDiff == 0) throw new RuntimeException("0 comparison in basictask between " + this + " and " + that)
      return typeDiff
    }

    def execute() = {
      //      val nbExec = execCounter.incrementAndGet
      //      if (nbExec > 1) throw new RuntimeException("multiple executions of the same task")
      //      System.gc()
      //      Thread.sleep(1000)
      //      val rt = Runtime.getRuntime
      //      println("max memory " + rt.maxMemory() / 1000000000d + " total " + rt.totalMemory() / 1000000000d + " free " + rt.freeMemory() / 1000000000d + " used " + (rt.totalMemory() - rt.freeMemory()) / 1000000000d)
      //      val nio = sun.misc.SharedSecrets.getJavaNioAccess().getDirectBufferPool()
      //      println("native memory count " + nio.getCount + " used " + nio.getMemoryUsed / 1000000000d + " capacity " + nio.getTotalCapacity / 1000000000d)
//      println(Thread.currentThread().getName + " going to execute " + t + " on " + collector.prefix + " with " + edgeExtension)
      //      DFSCode.synchronized {
      //        print(collector.prefix.extend(edgeExtension).getCFLRepresentation)
      //      }
      val d1g = d1.get
      val d2g = d2.get

      val sw: StopWatch = new StopWatch()
      sw.start()
      //      if (collector.prefix.toString().contains("Edge (0, 1, 1, 0, 1, 0)  F ,Edge (1, 2, 1, 0, 5, 0)  F ,Edge (2, 3, 5, 0, 5, 0)  F )") && edgeExtension.toString().contains("(3, 4, 5, 0, 5, 0)")) {
      //        println("dumping")
      //        //        println(d1g.MNISupport)
      //        //        d1g.asInstanceOf[AutomaTree].describe
      //        //        println(d2g.MNISupport)
      //        //        d2g.asInstanceOf[AutomaTree].describe
      //        Marshal.dumpToFile("long_" + collector.prefix.hashCode() + "_" + edgeExtension.hashCode() + ".dag1", d1g)
      //        Marshal.dumpToFile("long_" + collector.prefix.hashCode() + "_" + edgeExtension.hashCode() + ".dag2", d2g)
      //      }
      // val interestingCode = new DFSCode(Vector(Edge.makeOutEdge(0,1,0,948278,0),Edge.makeInEdge(1,2,0,948278,0),Edge.makeOutEdge(2,3,0,948285,0), Edge.makeOutEdge(0,4,0,948285,0)),5)
      // val interestingAncestors = interestingCode.getAncestors()
      //
      // val c = collector.prefix.extend(edgeExtension)
      // if(interestingAncestors.contains(c)){
      //   println(Thread.currentThread().getName + " dumping interesting code parents to file: "+ c)
      //   Marshal.dumpToFile("ancestors_" + c.hashCode() + ".dag1", d1g)
      //   Marshal.dumpToFile("ancestors_" + c.hashCode() + ".dag2", d2g)
      // }
      val automorphTransfers = collector.prefix.extend(edgeExtension).getAutomorphismMNITransfers
      val res: DagProvider = try {
        if (d1g != null && d2g != null) {
          t match {
            case IntersectType.FFtoFF => {
              val doubleFFpos = if (edgeExtension.i == collector.prefix.getEdgeAt(collector.prefix.codeLength() - 1).i) edgeExtension.i else -1
              d1g.intersectFFtoFF(d2g, minSup, collector.prefix.codeLength == maxSize - 1, automorphTransfers, doubleFFpos)
            }
            case IntersectType.FFtoFB => {
              val lastEdge = collector.prefix.getEdgeAt(collector.prefix.codeLength() - 1)
              d1g.intersectFFtoFB(d2g, minSup, collector.prefix.codeLength == maxSize - 1, automorphTransfers, lastEdge.iLabel != edgeExtension.iLabel || lastEdge.jLabel != edgeExtension.jLabel || lastEdge.edgeLabel != edgeExtension.edgeLabel)
            }
            case IntersectType.BBtoBB => d1g.intersectBBtoBB(d2g, minSup, collector.prefix.codeLength == maxSize - 1, automorphTransfers)
            case IntersectType.BFtoBF => {
              d1g.intersectBFtoBF(d2g, minSup, collector.prefix.codeLength == maxSize - 1, automorphTransfers)
            }
            case IntersectType.extend => d1g.extend(d2g, minSup, collector.prefix.codeLength == maxSize - 1, automorphTransfers)
          }
        } else {
          null
        }
      } catch {
        case e: Exception => {
          e.printStackTrace()
          println(Thread.currentThread().getName + " exception " + t + " on " + collector.prefix + " (" + collector.prefix.getNbNodes() + ") " + " with " + edgeExtension + " took " + sw.getElapsedTime() + " ms")
          Marshal.dumpToFile("exception_" + t + "_" + collector.prefix.hashCode() + "_" + edgeExtension.hashCode() + ".dag1", d1g)
          Marshal.dumpToFile("exception_" + t + "_" + collector.prefix.hashCode() + "_" + edgeExtension.hashCode() + ".dag2", d2g)
          System.exit(-1)
          null
        }
      }
      //      if (collector.prefix.extend(edgeExtension).toString().contains("(Edge (0, 1, 16, 1, 20, 0)  F ,Edge (1, 2, 20, 1, 16, 0)  F ,Edge (2, 3, 16, 1, 36, 0)  F ,Edge (3, 4, 36, 1, 16, 0)  F ,Edge (0, 5, 16, 1, 36, 0)  F )")) {
      //        println("dumping")
      //        Marshal.dumpToFile("ancestors_" + collector.prefix.hashCode() + "_" + edgeExtension.hashCode() + ".old.dag1", d1g)
      //        Marshal.dumpToFile("ancestors_" + collector.prefix.hashCode() + "_" + edgeExtension.hashCode() + ".old.dag2", d2g)
      //      }
      //      if (res != null && t == IntersectType.FFtoFF && res.MNISupport == 17168) {
      //        println(Thread.currentThread().getName + " dumping interesting code parents to file")
      //        Marshal.dumpToFile("ancestors_adfa" + ".dag1", d1g)
      //        Marshal.dumpToFile("ancestors_adfa" + ".dag2", d2g)
      //      }
      // val nbStatesTrans = res.getNbEmbeddingStorageUnits
      // println("STATS supp " + res.MNISupport + " nbVertices " + collector.prefix.getNbNodes() + " nbEdges " + (collector.prefix.codeLength()+1) + " nbEmbeddings " + res.nbEmbeddings + " nbStates " + nbStatesTrans(0) + " nbTransitions " + nbStatesTrans(1))

//      if (sw.getElapsedTime() > 0) {
//        println(Thread.currentThread().getName + " completed " + t + " on " + collector.prefix + " (" + collector.prefix.getNbNodes() + ") " + " with " + edgeExtension + " took " + sw.getElapsedTime() + " ms")
////        if (sw.getElapsedTime() > 600000) {
////          Marshal.dumpToFile("long_" + t + "_" + collector.prefix.hashCode() + "_" + edgeExtension.hashCode() + ".dag1", d1g)
////          Marshal.dumpToFile("long_" + t + "_" + collector.prefix.hashCode() + "_" + edgeExtension.hashCode() + ".dag2", d2g)
////        }
//      }

      //            println(Thread.currentThread().getName + " completed " + t + " on " + collector.prefix + " (" + collector.prefix.getNbNodes() + ") " + " with " + edgeExtension)
      collector.collect(res, edgeExtension)
    }
  }

  class ResultsCollector(val prefix: DFSCode) {
    @volatile private var resCollectedSoFar = 0
    private var expectedResults: Int = 0
    @volatile private var collectedResults: List[(Edge, DagProvider)] = Nil

    def addExpectedResults(nbAsyncTasks: Int) = {
      this.expectedResults = resCollectedSoFar + nbAsyncTasks
    }
    def collect(res: DagProvider, edgeExtension: Edge) = {
      //      println("collecting result " + prefix + " with " + edgeExtension)
      var lastResultCollected = false
      this.synchronized {
        resCollectedSoFar += 1
        if (resCollectedSoFar == expectedResults) lastResultCollected = true
        //        val dup = collectedResults.toMap.getOrElse(edgeExtension, null)
        //        if (dup != null) {
        //          throw new RuntimeException("dup in collector " + prefix + "\t" + edgeExtension + "\t" + dup + "\t" + res + "\t" + t)
        //        }
        //        println(Thread.currentThread().getName + " collected a results for " + prefix + ": " + edgeExtension + " " + resCollectedSoFar + "/" + expectedResults)
        if (res != null && res.isCanonical && res.get.MNISupport >= minSup) {
          addPattern(new FrequentGraph(prefix.extend(edgeExtension), res.get.MNISupport))
          if (prefix.codeLength() < maxSize - 1) this.collectedResults = (edgeExtension, res) :: collectedResults
        } else if (res != null && !res.isCanonical) {
          if (prefix.codeLength() < maxSize - 1) this.collectedResults = (edgeExtension, res) :: collectedResults
        }
      }
      if (lastResultCollected && !this.collectedResults.isEmpty) {
        //        println(Thread.currentThread().getName + " collected all results for " + prefix)
        if (prefix.codeLength() < maxSize - 1) {
          //          println(Thread.currentThread().getName + " adding prefix task " + cCode)

          //debug: print prefix task list
          writeWorkFile()
          //          println("collected all results for " + prefix + ": " + collectedResults.map(_._1))
          prefixTasks.add(new PrefixTask(Workpool.this, prefix, this.collectedResults))
        }
      }
    }
  }

  def newBasicTask(collector: Workpool#ResultsCollector, t: IntersectType, d1: DagProvider, d2: DagProvider, edgeExtension: Edge): BasicTask = {
    new BasicTask(collector.asInstanceOf[ResultsCollector], t, d1, d2, edgeExtension)
  }
  def newResultsCollector(prefix: DFSCode): ResultsCollector = {
    new ResultsCollector(prefix)
  }
  def addBasicTask(task: Workpool#BasicTask) {
    basicTasks.add(task.asInstanceOf[BasicTask])
    //debug: print prefix task list
    writeWorkFile()
  }

  def writeWorkFile() {
    // val file = new File("RemoteWorkpoolImpl." + localNodeName)
    // val bw = new BufferedWriter(new FileWriter(file))
    // bw.write("WorkFile for " + localNodeName + "(" + localNodeAddr + ")" + "\n")
    // bw.write("basicTasks.size: " + basicTasks.size + "\n")
    // bw.write("delayedTasks.size: " + delayedTasks.size + "\n")
    // prefixTasks.asScala.foreach(x => bw.write(x.toString + "\n"))
    // bw.close()
  }

}

trait RemoteWorkpool extends java.rmi.Remote {
  @throws[java.rmi.RemoteException]
  def hasAnyTasks(): Boolean
  @throws[java.rmi.RemoteException]
  def peekTask(): DFSCode
  @throws[java.rmi.RemoteException]
  def stealTask(id: String): StolenTask
  @throws[java.rmi.RemoteException]
  def stealMore(id: String, c: DFSCode): StolenTask
  @throws[java.rmi.RemoteException]
  def askShutdown(): Boolean
}

class RemoteWorkpoolImpl(workpool: Workpool) extends RemoteWorkpool {
  override def askShutdown(): Boolean = {
    return workpool.wantToShutDown
  }

  override def stealMore(id: String, c: DFSCode): StolenTask = {
    val t = workpool.tasksBeingStolen.get(c)
    if (t == null) {
      workpool.dagsCachedByRemoteExecutors.remove(id)
      return null
    }
    val split = t.splitSubtask
    if (split == null) {
      workpool.tasksBeingStolen.remove(c)
      workpool.dagsCachedByRemoteExecutors.remove(id)
      return null
    }
    if (split.extensions == null) return split
    val cachedDags = workpool.dagsCachedByRemoteExecutors.get(id)
    split.extensions.keySet().removeAll(cachedDags)
    cachedDags.addAll(split.extensions.keySet())
    if (!cachedDags.add(split.p1)) {
      split.d1 = null
    }
    return split
  }

  override def peekTask(): DFSCode = {

    //debug: print prefix task list
    workpool.writeWorkFile()
    while (true) {
      val lastPrefixTask = try {
        workpool.prefixTasks.last()
      } catch {
        case nse: NoSuchElementException => return null
      }
      val lookupCode = new DFSCode(Vector.fill(lastPrefixTask.prefix.codeLength())(Edge.makeEdge(-1, -1, -1, -1, -1, 1)), 2)
      val lookupTask = new PrefixTask(null, lookupCode, null)
      val t = workpool.prefixTasks.ceiling(lookupTask)
      if (t != null) {
        val p = t.peek
        if (p != null) return p
        workpool.prefixTasks.remove(t)
      }
    }
    return null
  }

  override def stealTask(id: String): StolenTask = {
    while (true) {
      val lastPrefixTask = try {
        workpool.prefixTasks.last()
      } catch {
        case nse: NoSuchElementException => null
      }
      if (lastPrefixTask == null) return null
      val lookupCode = new DFSCode(Vector.fill(lastPrefixTask.prefix.codeLength())(Edge.makeEdge(-1, -1, -1, -1, -1, 1)), 2)
      val lookupTask = new PrefixTask(null, lookupCode, null)
      var stolenTask = workpool.prefixTasks.ceiling(lookupTask)
      if (!stolenTask.beingStolen) {
        if (workpool.prefixTasks.remove(stolenTask)) {
          stolenTask.beingStolen = true

          //debug: print prefix task list
          workpool.writeWorkFile()

          workpool.prefixTasks.add(stolenTask)
          workpool.tasksBeingStolen.put(stolenTask.prefix, stolenTask)
        }
      }
      workpool.dagsCachedByRemoteExecutors.put(id, new java.util.HashSet)
      return stealMore(id, stolenTask.prefix)
    }
    return null
  }

  def hasAnyTasks(): Boolean = workpool.basicTasks.size > 0 || workpool.prefixTasks.size > 0
}

class StolenTask(@transient var workpool: Workpool, val prefix: DFSCode, var p1: Edge, var d1: DagProvider, var extensions: java.util.Map[Edge, DagProvider]) extends Serializable {

  def genBasicTasks(): Boolean = {
    if (d1 == null) d1 = DagProvider.graphData.getExtensions(p1)
    var (c, cDag) = (p1, d1)
    val cCode = prefix.extend(c)
    var createdTasks: List[Workpool#BasicTask] = Nil
    val collector: Workpool#ResultsCollector = workpool.newResultsCollector(cCode)
    import scala.collection.JavaConverters.mapAsJavaMapConverter
    val usedExtensions = if (extensions == null) DagProvider.graphData.getGraphMap.asJava else extensions
    val extensionsIter = usedExtensions.entrySet().iterator()
    while (extensionsIter.hasNext()) {
      val p = extensionsIter.next()
      val e = (p.getKey, p.getValue)

      if (c.isForwardEdge() && e._1.isForwardEdge()) {
        if (e._1.i <= c.i) {
          val ceCode = cCode.extend(e._1.shiftFF)
          if (ceCode != null) {
            if (ceCode.isCanonical) {
              createdTasks = workpool.newBasicTask(collector, IntersectType.FFtoFF, cDag, e._2, e._1.shiftFF) :: createdTasks
            } else {
              if (ceCode.hasPotential) {
                val doubleFFpos = if (e._1.i == c.i) e._1.i else -1
                collector.collect(new IntersectDag(cDag, e._2, IntersectType.FFtoFF, workpool.minSup, false, ceCode.getAutomorphismMNITransfers, doubleFFpos), e._1.shiftFF)
              }
            }
          }
        }

        if (c != e._1 && e._1.i <= c.i) {
          val ceCode = cCode.extend(e._1.shiftFB)
          if (ceCode != null) {
            if (ceCode.isCanonical) {
              createdTasks = workpool.newBasicTask(collector, IntersectType.FFtoFB, cDag, e._2, e._1.shiftFB) :: createdTasks
            }
          }
        }
      } else {
        if (!c.isForwardEdge()) {
          if (!e._1.isForwardEdge()) {
            if (c != e._1) {
              val ceCode = cCode.extend(e._1)
              if (ceCode != null) {
                if (ceCode.isCanonical) {
                  createdTasks = workpool.newBasicTask(collector, IntersectType.BBtoBB, cDag, e._2, e._1) :: createdTasks
                }
              }
            }
          } else if (e._2.isCanonical) {
            val ceCode = cCode.extend(e._1)
            if (ceCode != null) {
              if (ceCode.isCanonical) {
                createdTasks = workpool.newBasicTask(collector, IntersectType.BFtoBF, cDag, e._2, e._1) :: createdTasks
              } else {
                if (ceCode.hasPotential)
                  collector.collect(new IntersectDag(cDag, e._2, IntersectType.BFtoBF, workpool.minSup, false, ceCode.getAutomorphismMNITransfers, -1), e._1)
              }
            }
          }
        }
      }
    }
    if (c.isForwardEdge()) {
      val leafLabel = c.jLabel
      workpool.inputGraph.getExtensions(cDag.get.leafSet, leafLabel, workpool.minSup)
        .foreach {
          case (edge, instances) =>
            {
              val ceCode: DFSCode = cCode.extend(edge.shiftBy(cCode.getNbNodes() - 1))
              if (ceCode == null) {
                throw new Exception("genExtensionsAS, unexpected null ceCode making joins " + cCode + " with + " + edge.shiftBy(cCode.getNbNodes() - 1) + " nb nodes in prefix " + cCode.getNbNodes())
              }
              if (ceCode.hasPotential) {
                if (ceCode.isCanonical) {
                  createdTasks = workpool.newBasicTask(collector, IntersectType.extend, cDag, instances, edge.shiftBy(cCode.getNbNodes() - 1)) :: createdTasks
                } else {
                  if (ceCode.hasPotential)
                    collector.collect(new JoinDag(cDag.get, edge, workpool.minSup, ceCode.getAutomorphismMNITransfers), edge.shiftBy(cCode.getNbNodes() - 1))
                }
              }
            }
        }
    }
    if (createdTasks.isEmpty) return false
    collector.addExpectedResults(createdTasks.size)
    createdTasks.foreach(workpool.addBasicTask(_))
    return true
  }
}

class PrefixTask(@transient var workpool: Workpool, val prefix: DFSCode, @volatile var outerLoop: List[(Edge, DagProvider)], var innerLoop: Iterable[(Edge, DagProvider)]) extends Ordered[PrefixTask] with Serializable {

  var beingStolen = false

  def this(workpool: Workpool, prefix: DFSCode, seeds: List[Edge], g: GraphInfo) {
    this(workpool, prefix, seeds.map(e => (e, new SeedDag(e))), g.getGraph.map(p => (p._1, new SeedDag(p._1))))
  }

  def this(workpool: Workpool, prefix: DFSCode, extensions: List[(Edge, DagProvider)]) {
    this(workpool, prefix, if (extensions == null) null else extensions.filter(ext => {
      val extendedCode = prefix.extend(ext._1)
      extendedCode == null || extendedCode.isCanonical
    }), extensions)
    //    if (this.outerLoop.map(_._1).toSet.size != this.outerLoop.size) {
    //      throw new RuntimeException("duplicate in outerloop " + this.outerLoop)
    //    }
    //    if (this.innerLoop.map(_._1).toSet.size != this.innerLoop.size) {
    //      throw new RuntimeException("duplicate in outerloop " + this.innerLoop)
    //    }
  }

  def compare(that: PrefixTask): Int = {
    if (this == that) return 0
    if (this.prefix.codeLength() < that.prefix.codeLength) return 1
    if (this.prefix.codeLength() > that.prefix.codeLength) return -1
    if (this.beingStolen && !that.beingStolen) return 1
    if (!this.beingStolen && that.beingStolen) return -1
    val codeDiff = this.prefix.compare(that.prefix)
    if (codeDiff != 0) return codeDiff
    if (this.innerLoop != null && that.innerLoop == null) return 1
    if (this.innerLoop == null && that.innerLoop != null) return -1
    throw new RuntimeException("0 comparison in prefix task between " + this + " and " + that)
  }

  override def toString(): String = {
    return "PrefixTask[" + prefix + "-" + beingStolen + "-" + outerLoop + "-" + innerLoop + "]"
  }

  def splitSubtask: StolenTask = {
    while (true) {
      var (c, cDag) = this.synchronized {
        if (outerLoop.isEmpty) return null
        val r = outerLoop.head
        outerLoop = outerLoop.tail
        r
      }
      if (prefix.codeLength() == 0) return new StolenTask(null, prefix, c, null, null)
      val cCode = this.prefix.extend(c)
      val edgesWithPotential = innerLoop.map(e => (e, extensionPotential(cCode, c, cDag, e))).filter(p => p._2 >= 0)
      if (c.isForwardEdge() || edgesWithPotential.exists(p => p._2 == 1)) {
        val selectedEdges: java.util.HashMap[Edge, DagProvider] = new java.util.HashMap()
        edgesWithPotential.foreach(p => selectedEdges.put(p._1._1, p._1._2))
        println(InetAddress.getLocalHost().getHostAddress() + " splitting a task " + cCode + " with known extensions " + selectedEdges.keySet)
        return new StolenTask(null, prefix, c, cDag, selectedEdges)
      }
    }
    return null
  }

  def peek: DFSCode = {
    if (outerLoop.isEmpty) return null
    val r = outerLoop.head
    return this.prefix.extend(r._1)
  }

  def extensionPotential(cCode: DFSCode, c: Edge, cDag: DagProvider, e: (Edge, DagProvider)): Byte = {
    var res: Byte = -1
    if (c.isForwardEdge() && e._1.isForwardEdge()) {
      if (e._1.i <= c.i) {
        val ceCode = cCode.extend(e._1.shiftFF)
        if (ceCode != null) {
          // if (ceCode.isCanonical && interestingAncestors.contains(ceCode)) {
          if (ceCode.isCanonical) {
            return 1
          } else {
            if (ceCode.hasPotential)
              res = Math.max(res, 0).toByte
          }
        }
      }

      if (c != e._1 && e._1.i <= c.i) {
        val ceCode = cCode.extend(e._1.shiftFB)
        if (ceCode != null) {
          // if (ceCode.isCanonical && interestingAncestors.contains(ceCode)) {
          if (ceCode.isCanonical) {
            return 1
          }
        }
      }
    } else {
      if (!c.isForwardEdge()) {
        if (!e._1.isForwardEdge()) {
          if (c != e._1) {
            val ceCode = cCode.extend(e._1)
            if (ceCode != null) {
              if (ceCode.isCanonical) {
                return 1
              }
            }
          }
        } else if (e._2.isCanonical) {
          val ceCode = cCode.extend(e._1)
          if (ceCode != null) {
            // if (ceCode.isCanonical && interestingAncestors.contains(ceCode)) {
            if (ceCode.isCanonical) {
              return 1
            } else {
              if (ceCode.hasPotential)
                res = Math.max(res, 0).toByte
            }
          }
        }
      }
    }
    if (c.isForwardEdge()) {
      val leafLabel = c.jLabel
      workpool.inputGraph.getExtensions(cDag.get.leafSet, leafLabel, workpool.minSup)
        .foreach {
          case (edge, instances) =>
            {
              val ceCode: DFSCode = cCode.extend(edge.shiftBy(cCode.getNbNodes() - 1))
              if (ceCode == null) {
                throw new Exception("genExtensionsAS, unexpected null ceCode making joins " + cCode + " with + " + edge.shiftBy(cCode.getNbNodes() - 1) + " nb nodes in prefix " + cCode.getNbNodes())
              }
              if (ceCode.hasPotential) {
                // if (ceCode.isCanonical && interestingAncestors.contains(ceCode)) {
                if (ceCode.isCanonical) {
                  return 1
                } else {
                  if (ceCode.hasPotential)
                    res = Math.max(res, 0).toByte
                }
              }
            }
        }
    }
    return res
  }

  def genBasicTasks(): Boolean = {
    while (true) {
      var (c, cDag) = this.synchronized {
        if (outerLoop.isEmpty) return false
        val r = outerLoop.head
        outerLoop = outerLoop.tail
        r
      }

      val cCode = prefix.extend(c)
      var createdTasks: List[Workpool#BasicTask] = Nil
      val collector: Workpool#ResultsCollector = workpool.newResultsCollector(cCode)
      innerLoop.foreach {
        e =>
          if (c.isForwardEdge() && e._1.isForwardEdge()) {
            if (e._1.i <= c.i) {
              val ceCode = cCode.extend(e._1.shiftFF)
              if (ceCode != null) {
                // if (ceCode.isCanonical && interestingAncestors.contains(ceCode)) {
                if (ceCode.isCanonical) {
                  //                  println("ceCode " + ceCode + " e._1 " + e._1)
                  createdTasks = workpool.newBasicTask(collector, IntersectType.FFtoFF, cDag, e._2, e._1.shiftFF) :: createdTasks
                } else {
                  if (ceCode.hasPotential) {
                    val doubleFFpos = if (e._1.i == c.i) e._1.i else -1
                    collector.collect(new IntersectDag(cDag, e._2, IntersectType.FFtoFF, workpool.minSup, false, ceCode.getAutomorphismMNITransfers, doubleFFpos), e._1.shiftFF)
                  }
                }
              }
            }

            if (c != e._1 && e._1.i <= c.i) {
              val ceCode = cCode.extend(e._1.shiftFB)
              if (ceCode != null) {
                // if (ceCode.isCanonical && interestingAncestors.contains(ceCode)) {
                if (ceCode.isCanonical) {
                  createdTasks = workpool.newBasicTask(collector, IntersectType.FFtoFB, cDag, e._2, e._1.shiftFB) :: createdTasks
                }
              }
            }
          } else {
            if (!c.isForwardEdge()) {
              if (!e._1.isForwardEdge()) {
                if (c != e._1) {
                  val ceCode = cCode.extend(e._1)
                  if (ceCode != null) {
                    if (ceCode.isCanonical) {
                      createdTasks = workpool.newBasicTask(collector, IntersectType.BBtoBB, cDag, e._2, e._1) :: createdTasks
                    }
                  }
                }
              } else if (e._2.isCanonical) {
                val ceCode = cCode.extend(e._1)
                if (ceCode != null) {
                  // if (ceCode.isCanonical && interestingAncestors.contains(ceCode)) {
                  if (ceCode.isCanonical) {
                    createdTasks = workpool.newBasicTask(collector, IntersectType.BFtoBF, cDag, e._2, e._1) :: createdTasks
                  } else {
                    if (ceCode.hasPotential)
                      collector.collect(new IntersectDag(cDag, e._2, IntersectType.BFtoBF, workpool.minSup, false, ceCode.getAutomorphismMNITransfers, -1), e._1)
                  }
                }
              }
            }
          }
      }
      if (c.isForwardEdge()) {
        val leafLabel = c.jLabel
        workpool.inputGraph.getExtensions(cDag.get.leafSet, leafLabel, workpool.minSup)
          .foreach {
            case (edge, instances) =>
              {
                //                println("considering extend " + cCode + " with " + edge)
                val ceCode: DFSCode = cCode.extend(edge.shiftBy(cCode.getNbNodes() - 1))
                if (ceCode == null) {
                  throw new Exception("genExtensionsAS, unexpected null ceCode making joins " + cCode + " with + " + edge.shiftBy(cCode.getNbNodes() - 1) + " nb nodes in prefix " + cCode.getNbNodes())
                }
                if (ceCode.hasPotential) {
                  // if (ceCode.isCanonical && interestingAncestors.contains(ceCode)) {
                  if (ceCode.isCanonical) {
                    createdTasks = workpool.newBasicTask(collector, IntersectType.extend, cDag, instances, edge.shiftBy(cCode.getNbNodes() - 1)) :: createdTasks
                  } else {
                    if (ceCode.hasPotential)
                      collector.collect(new JoinDag(cDag.get, edge, workpool.minSup, ceCode.getAutomorphismMNITransfers), edge.shiftBy(cCode.getNbNodes() - 1))
                  }
                }
              }
          }
      }
      //      if (createdTasks.map(t => t.edgeExtension).toSet.size != createdTasks.size) {
      //        throw new RuntimeException("duplicate")
      //      }
      //      if (collector.prefix == new DFSCode(List(Edge.makeEdge(0, 1, 0, 940190, 0, 1), Edge.makeEdge(1, 2, 0, 940190, 0, -1), Edge.makeEdge(2, 3, 0, 944668, 0, 1), Edge.makeEdge(2, 4, 0, 948227, 0, -1), Edge.makeEdge(1, 5, 0, 940190, 0, -1), Edge.makeEdge(5, 6, 0, 944668, 0, 1)), 7)) {
      //        println("tasks for monitored " + createdTasks.map(t => t.edgeExtension))
      //      }
      //      println(Thread.currentThread().getName + " created " + createdTasks.size + " basic tasks")
      if (!createdTasks.isEmpty) {
        collector.addExpectedResults(createdTasks.size)
        createdTasks.foreach(workpool.addBasicTask(_))
        return true
      }
    }
    return false
  }
}
