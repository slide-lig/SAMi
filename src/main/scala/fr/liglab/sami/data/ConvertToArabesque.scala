package fr.liglab.sami.data

import java.io.BufferedReader
import java.io.BufferedWriter
import java.io.FileReader
import java.io.FileWriter
import java.util.TreeMap
import net.openhft.koloboke.collect.set.IntSet
import net.openhft.koloboke.collect.set.hash.HashIntSets
import scala.collection.immutable.SortedSet
import scala.collection.immutable.TreeSet

object ConvertToArabesque {
  def main(args: Array[String]): Unit = {
    var nextId = -1
    val idMapping = scala.collection.mutable.Map.empty[Int, Int]
    val labels = scala.collection.mutable.Map.empty[Int, Int]
    val neighbors = scala.collection.mutable.Map.empty[Int, IntSet]
    def getId(v: Int): Int = {
      if (idMapping.contains(v)) {
        return idMapping(v)
      } else {
        nextId += 1
        idMapping(v) = nextId
        return nextId
      }
    }
    def setNeighbors(v1: Int, v2: Int) = {
      if (!neighbors.contains(v1)) {
        neighbors.put(v1, HashIntSets.newMutableSet)
      }
      if (!neighbors.contains(v2)) {
        neighbors.put(v2, HashIntSets.newMutableSet)
      }
      neighbors(v1).add(v2)
      neighbors(v2).add(v1)
    }
    println("reading")
    val br = new BufferedReader(new FileReader(args(0)))
    var line: String = null
    do {
      line = br.readLine()
      if (line != null) {
        val sp = line.split("\t", 3)
        if (sp.size <= 2) {
          return
        } else {
          val vId = getId(sp(0).toInt)
          val vLbl = sp(1).toInt
          labels(vId) = vLbl
          val spe = sp(2).split('\t')
          for (i <- 0 until spe.length by 3) {
            val neighborId = getId(spe(i).toInt)
            val neighborLbl = spe(i + 1).toInt
            labels(neighborId) = neighborLbl
            setNeighbors(vId, neighborId)
          }
        }
      }
    } while (line != null)
    br.close()
    println("writing")
    val bw = new BufferedWriter(new FileWriter(args(0) + ".arabesque"))
    val allVertexIds = new TreeSet[Int]() ++ idMapping.keys
    for (vId <- allVertexIds) {
      bw.write(vId + " " + labels(vId))
      val neigh = neighbors.get(vId)
      if (neigh.isDefined) {
        val c = neigh.get.cursor()
        val sortedNeighborsList = scala.collection.mutable.TreeSet.empty[Int]
        while (c.moveNext()) {
          sortedNeighborsList += c.elem()
        }
        for (n <- sortedNeighborsList) {
          bw.write(" " + n)
        }
      }
      bw.write("\n")
    }
    bw.close()
  }
}