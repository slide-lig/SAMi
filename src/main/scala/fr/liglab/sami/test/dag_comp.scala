package fr.liglab.sami.test

import java.io._

import scala.language.postfixOps

import fr.liglab.sami.code._
import fr.liglab.sami.utilities._
import net.openhft.koloboke.collect.set.IntSet
import net.openhft.koloboke.collect.set.hash.HashIntSets
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.Files

object dag_comp_runner {

  def main(args: Array[String]): Unit = {

    if (args.size < 2) {
      println("args: dag1 dag2 threshold")
    }

    //    val t1File = args(0)
    //    val t2File = args(1)
    //    val threshold = args(2).toInt
    val oldp1: AutomaTree = Marshal.readFromFile[AutomaTree](args(0))
    val oldp2: AutomaTree = Marshal.readFromFile[AutomaTree](args(1))
    val newp1: AutomaTreeNodeFilter = Marshal.readFromFile[AutomaTreeNodeFilter](args(2))
    val newp2: AutomaTreeNodeFilter = Marshal.readFromFile[AutomaTreeNodeFilter](args(3))
    newp1.parallelValidation = false
    newp2.parallelValidation = false
    println("data loaded")
    val oldD = oldp1.intersectFFtoFF(oldp2, 17000, false, null, -1).asInstanceOf[AutomaTree]
    //    println(oldD.MNISupport)
    //    oldD.describe
    val newD = newp1.intersectFFtoFF(newp2, 17000, false, null, -1).asInstanceOf[AutomaTreeNodeFilter]
    //    println(newD.MNISupport)
    //    newD.describe
    val mniSetsOld = Array.fill(oldD.nbNodes)(HashIntSets.newMutableSet())
    val nblOld = oldD.getNodesByLevel()
    for (i <- 0 until nblOld.length) {
      for (n <- nblOld(i)) {
        mniSetsOld(i).addAll(n.transitions.keySet())
      }
    }
    val nblNew = newD.getNodesByLevel()
    for (i <- 0 until nblNew.length) {
      for (n <- nblNew(i)) {
        mniSetsOld(i).removeAll(n.transitions.keySet())
      }
    }

    for (i <- 0 until mniSetsOld.length) {
      println(i + ": " + mniSetsOld(i))
    }

    //    val mniSets2: Array[IntSet] = Array.fill(p1.nbNodes)(HashIntSets.newMutableSet())
    //    val nbl2 = p2.mniSetsFromEmbeddings(mniSets2)
    //    for (i <- 0 until nbl2.length) {
    //      for (n <- nbl2(i)) {
    //        mniSets2(i).addAll(n.transitions.keySet())
    //      }
    //    }
    //    for (p <- p2.paths) {
    //      for (i <- 0 until p.size) {
    //        mniSets2(i).add(p(i))
    //      }
    //    }
    //
    //    val nbl2 = p2.getNodesByLevel()
    //    for (i <- 0 until nbl2.length) {
    //      for (n <- nbl2(i)) {
    //        mniSets2(i).addAll(n.transitions.keySet())
    //      }
    //    }
    //    for (i <- 0 until mniSets1.size) {
    //      println(mniSets1(i).size() + "\t" + mniSets2(i).size())
    //      //      mniSets1(i).removeAll(mniSets2(i))
    //      mniSets2(i).removeAll(mniSets1(i))
    //      println(mniSets2(i))
    //    }
  }
}
