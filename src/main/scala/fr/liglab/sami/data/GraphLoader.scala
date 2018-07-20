package fr.liglab.sami.data

import java.util.HashMap
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

import scala.io.Source

import fr.liglab.sami.code.IncidentEdge
import fr.liglab.sami.code.AutomaTreeNodeFilter
import fr.liglab.sami.code.DAG
import fr.liglab.sami.code.Edge
import net.openhft.koloboke.collect.map.IntObjMap
import net.openhft.koloboke.collect.map.hash.HashIntObjMaps
import net.openhft.koloboke.collect.set.IntSet
import net.openhft.koloboke.collect.set.hash.HashIntSets

class GraphLoader {

  def loadAsEdgeDag(path: String, directed: Boolean, minSupport: Int, parallelValidation: Boolean): scala.collection.immutable.Map[Edge, DAG] = {
    val linesDoneCounter = new AtomicInteger(0)
    val edgeFreq = new ConcurrentHashMap[Edge, (IntSet, IntSet)]()
    val edgeTypes = new HashMap[Edge, IntObjMap[IntSet]]()
    def preProcessLine(l: String): Unit = {
      val sp = l.split("\t", 3)
      if (sp.size > 2) {
        val vId = sp(0).toInt
        val vLbl = sp(1).toInt
        val spe = sp(2).split('\t')
        for (i <- 0 until spe.length by 3) {
          val neighborId = spe(i).toInt
          val neighborLbl = spe(i + 1).toInt
          val edgeLbl = spe(i + 2).toInt
          val (e1, e2) = if (directed) {
            (Edge.makeOutEdge(0, 1, vLbl, edgeLbl, neighborLbl), Edge.makeInEdge(0, 1, neighborLbl, edgeLbl, vLbl))
          } else {
            (Edge.makeUndirEdge(0, 1, vLbl, edgeLbl, neighborLbl), Edge.makeUndirEdge(0, 1, neighborLbl, edgeLbl, vLbl))
          }
          val p1 = (vId, neighborId)
          val p2 = (neighborId, vId)
          var l = edgeFreq.get(e1)
          if (l == null) {
            l = (HashIntSets.newMutableSet(), HashIntSets.newMutableSet())
            val ret = edgeFreq.putIfAbsent(e1, l)
            if (ret != null) l = ret
          }
          l.synchronized {
            //            if (l._1.size < minSupport)
            l._1.add(vId)
            //            if (l._2.size < minSupport)
            l._2.add(neighborId)
            if (e1 == e2) {
              //              if (l._1.size < minSupport)
              l._1.add(neighborId)
              //              if (l._2.size < minSupport)
              l._2.add(vId)
            }
          }
          if (e1 != e2) {
            l = edgeFreq.get(e2)
            if (l == null) {
              l = (HashIntSets.newMutableSet(), HashIntSets.newMutableSet())
              val ret = edgeFreq.putIfAbsent(e2, l)
              if (ret != null) l = ret
            }
            l.synchronized {
              //              if (l._1.size < minSupport)
              l._1.add(neighborId)
              //              if (l._2.size < minSupport)
              l._2.add(vId)
            }
          }
        }
      }
      val nbDone = linesDoneCounter.incrementAndGet()
      if (nbDone % 1000000 == 0) println(nbDone + " lines pre-processed")
    }
    def buildDag(vToSuccessors: IntObjMap[IntSet]): DAG = {
      //      if (parallelValidation) {
      //        new ADFAPar(endPoints)
      new AutomaTreeNodeFilter(vToSuccessors, parallelValidation)
      //      } else {
      //        new ADFA(endPoints)
      //      }
    }
    println("loading in memory")
    val source = Source.fromFile(path)
    val lines = source.getLines().toSeq.par
    source.close
    println("filtering frequent edges")
    lines.foreach(preProcessLine)
    import scala.collection.JavaConverters._
    val frequentEdges = edgeFreq.asScala.filter(p => p._2._1.size >= minSupport && p._2._2.size >= minSupport).map(_._1).toSet /*.filter(e => (e.iLabel == 16 && e.jLabel == 20) || (e.jLabel == 16 && e.iLabel == 20))*/
    frequentEdges.foreach {
      e =>
        val m: IntObjMap[IntSet] = HashIntObjMaps.newMutableMap()
        edgeTypes.put(e, m)
    }
    frequentEdges.par.foreach {
      e =>
        val m: IntObjMap[IntSet] = edgeTypes.get(e)
        val cur = edgeFreq.get(e)._1.cursor()
        while (cur.moveNext()) {
          m.put(cur.elem(), HashIntSets.newMutableSet())
        }
      //        println("prepared " + m.size() + " for " + e)
    }
    //    edgeFreq.asScala.foreach { p =>
    //      {
    //        val sup = Math.min(p._2._1.size, p._2._2.size)
    //        if (sup > 10000) {
    //          println(p._1 + "\t" + sup)
    //        }
    //      }
    //    }
    println("selected edges " + frequentEdges.size)
    def processLine(l: String): Unit = {
      val sp = l.split("\t", 3)
      if (sp.size > 2) {
        val vId = sp(0).toInt
        val vLbl = sp(1).toInt
        val spe = sp(2).split('\t')
        for (i <- 0 until spe.length by 3) {
          val neighborId = spe(i).toInt
          val neighborLbl = spe(i + 1).toInt
          val edgeLbl = spe(i + 2).toInt
          val (e1, e2) = if (directed) {
            //TODO send just one and reverse dag for other
            (Edge.makeOutEdge(0, 1, vLbl, edgeLbl, neighborLbl), Edge.makeInEdge(0, 1, neighborLbl, edgeLbl, vLbl))
          } else {
            (Edge.makeUndirEdge(0, 1, vLbl, edgeLbl, neighborLbl), Edge.makeUndirEdge(0, 1, neighborLbl, edgeLbl, vLbl))
          }
          def addSuccessor(e: Edge, source: Int, dest: Int) {
            if (frequentEdges.contains(e)) {
              val l = edgeTypes.get(e)
              var nodeSuccessors = l.get(source)
              nodeSuccessors.synchronized {
                nodeSuccessors.add(dest)
              }
            }
          }
          addSuccessor(e1, vId, neighborId)
          addSuccessor(e2, neighborId, vId)
        }
      }
      val nbDone = linesDoneCounter.incrementAndGet()
      if (nbDone % 1000000 == 0) println(nbDone + " lines processed")
    }
    println("collecting edges")
    linesDoneCounter.set(0)
    lines.foreach(processLine)
    println("building dags")
    val freqEdges = edgeTypes.asScala.toSeq.par
    //.filter(_._2.size >= minSupport)
    //      .filter {
    //        x =>
    //          val v1s = HashIntSets.newMutableSet(minSupport)
    //          val v2s = HashIntSets.newMutableSet(minSupport)
    //          var frequent = false
    //          for (e <- x._2 if !frequent) {
    //            if (v1s.size() < minSupport) {
    //              v1s.add(e._1)
    //            }
    //            if (v2s.size() < minSupport) {
    //              v2s.add(e._2)
    //            }
    //            if (v1s.size == minSupport && v2s.size == minSupport) {
    //              frequent = true
    //            }
    //          }
    //          frequent
    //      }
    //    println(freqEdges.size + " freq edges")
    val dags = freqEdges.map(t => /*Future*/ {
      //      println("building dag " + t._1)
      val d = buildDag(t._2)
      //      println("done dag " + t._1)
      // useless since we already checked it was frequent in pre-processing
      //      if (d.MNISupport < minSupport) {
      //        null
      //      } else {
      (t._1, d)
      //      }
    }).filter(_ != null).seq.toMap
    //    val fut = Future.sequence(futureDags)
    //    val dags = Await.result(fut, Duration.Inf).filter(_ != null).toMap
    return dags
  }

  def loadAsMap(path: String, directed: Boolean): Map[Int, List[IncidentEdge]] = {
    // input format: <src_v_id> <src_v_lbl> [ <dst_v_id> <dst_v_lbl> <edge_lbl> ...]
    // expects all to be convertable to Ints, i.e. already indexed
    // expects if directed, that only outward edges to be included
    // expects an edge to be only represented once

    val incidents = Source.fromFile(path)
      .getLines
      .map(_.split("\t", 3))
      .filter(_.size > 2)
      .flatMap { arr =>
        val vId = arr(0).toInt
        val vLbl = arr(1).toInt
        arr(2).split('\t').sliding(3, 3).toList.flatMap { ne =>
          val neighborId = ne(0).toInt
          val neighborLbl = ne(1).toInt
          val edgeLbl = ne(2).toInt
          if (directed) {
            Array((vId, new IncidentEdge(neighborId, vLbl, edgeLbl, neighborLbl, Edge.DIR_OUT)),
              (neighborId, new IncidentEdge(vId, neighborLbl, edgeLbl, vLbl, Edge.DIR_IN)))
          } else {
            Array((vId, new IncidentEdge(neighborId, vLbl, edgeLbl, neighborLbl, Edge.DIR_ANY)),
              (neighborId, new IncidentEdge(vId, neighborLbl, edgeLbl, vLbl, Edge.DIR_ANY)))
          }
        }
      }.toSeq.groupBy(_._1).mapValues(s => s.map(_._2).toList).toMap

    return incidents

  }
}

object GraphLoaderRunner {

  def main(args: Array[String]): Unit = {
    val startTime = System.currentTimeMillis()
    val g = new GraphLoader().loadAsEdgeDag("graph_1.tsv", false, 24000, false)
    val endTime = System.currentTimeMillis()
    println("loading time " + (endTime - startTime))
    println(g.size)
    //    if (args.length == 0) {
    //      println("Missing: input")
    //      return
    //    }
    //
    //    println("Starting: SparkGraphLoaderRunner")
    //
    //    val conf = new SparkConf().setAppName("fr.liglab.sami.data.GraphLoaderRunner").setMaster("local")
    //    val sc = new SparkContext(conf)
    //
    //    val rdd = new GraphLoader().loadAsRDD(sc, args(0), false)
    //    println(s"#Vertices: ${rdd.count()}")
    //
    //    println("Finished: GraphLoaderRunner")

  }

}
