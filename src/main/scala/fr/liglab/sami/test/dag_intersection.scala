package fr.liglab.sami.test

import java.io._
import java.nio.file.Files
import java.nio.file.Paths

import scala.language.postfixOps

import fr.liglab.sami.code._
import fr.liglab.sami.utilities._
import scala.collection.mutable.ArrayBuffer

object dag_intersect_runner {

  def main(args: Array[String]): Unit = {

    if (args.size < 3) {
      //intersection
      println("args: intersectFFtoFF dag1 dag2 threshold")
      println("args: intersectFFtoFB dag1 dag2 threshold")
      println("args: intersectBBtoBB dag1 dag2 threshold")
      println("args: intersectBFtoBF dag1 dag2 threshold")
      //extend
      // prepare exts2...
      // cat long_running_join.exts | sed -e 's/List(//g' | sed -e 's/)//g' | sed -e 's/ //g' > long_running_join.exts2
      println("args: extend dag1 extensionsDag threshold")
    }

    val op = args(0)
    val t1File = args(1)
    val t2File = args(2)
    val threshold = args(3).toInt
    val p1: AutomaTreeNodeFilter = Marshal.readFromFile[AutomaTreeNodeFilter](t1File)
    //    val p1: AutomaTree = Marshal.load[DAG](bytes).asInstanceOf[AutomaTree]
    println(p1.MNISupport /*+ "\t" + p1.hasEmbedding(List(2023408, 2479691, 2059346, 2723979, 2306538))*/ )
    p1.describe
    if (op.startsWith("")) {
      val p2: AutomaTreeNodeFilter = Marshal.readFromFile[AutomaTreeNodeFilter](t2File)
      println(p2.MNISupport /* + "\t" + p2.hasEmbedding(List(2023408, 2479691, 2059346, 2723979, 2737777))*/ )
      p2.describe
      p1.parallelValidation = true
      p2.parallelValidation = true
      //      println("performing intersection")
      val sw = new StopWatch()
      sw.start()
      //      for (i <- 0 until 100) {
      val res = op match {
        //TODO fix
        case "intersectFFtoFF" => p1.intersectFFtoFF(p2, threshold, false, null, args(4).toInt)
        case "intersectFFtoFB" => p1.intersectFFtoFB(p2, threshold, false, null, false)
        case "intersectBBtoBB" => p1.intersectBBtoBB(p2, threshold, false, null)
        case "intersectBFtoBF" => p1.intersectBFtoBF(p2, threshold, false, null)
        case "extend"          => p1.extend(p2, threshold, false, null)
        case _                 => throw new RuntimeException("incorrect op")
      }
      //      println(res.MNISupport/* + "\t" + res.asInstanceOf[AutomaTree].hasEmbedding(List(2023408, 2479691, 2059346, 2723979, 2306538, 2737777))*/)
      println(res.MNISupport)
      //      Marshal.dumpToFile(args(1).replace("dag1", "child"),res)
      //      }
      //
      println("intersection complete: " + sw.getElapsedTime() + "ms")
      res.asInstanceOf[AutomaTreeNodeFilter].describe
      //      Marshal.dumpToFile("/Users/vleroy/workspace/graph/yago_long/p2_parent.c", res)
      //      val mniSets = Array.fill(res.depth)(HashIntSets.newMutableSet())
      //      describe(p1.asInstanceOf[AutomaTree])
      //      describe(p2.asInstanceOf[AutomaTree])
      //      describe(res.asInstanceOf[AutomaTree])
      //      for (l <- mniSets) {
      //        println(l.size)
      //      }
    } else {
      //      val exts: Map[Int, Seq[Int]] = Source.fromFile(t2File).getLines().map { line => val kv = line.split(":"); (kv(0).toInt, kv(1).split(",").map(_.toInt).toSeq) }.toMap
      //
      //      println("performing join")
      //      val sw = new StopWatch()
      //      sw.start()
      //      val res: ADFA = p1.extend(exts, threshold).asInstanceOf[ADFA]
      //      println("intersection complete: " + sw.getElapsedTime() + "ms")
    }

  }
}
