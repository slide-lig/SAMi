package fr.liglab.sami.data

import java.io.BufferedReader
import java.io.FileReader
import java.io.BufferedWriter
import java.io.FileWriter
import java.util.HashMap
import java.util.TreeMap
import net.openhft.koloboke.collect.set.hash.HashIntSets
import net.openhft.koloboke.collect.map.IntIntMap
import org.eclipse.collections.impl.factory.primitive.IntIntMaps
import org.eclipse.collections.impl.map.mutable.primitive.IntIntHashMap
import scala.util.Random

object ConvertAdjacencyList {

  def main(args: Array[String]): Unit = {
    val nbGroups = args(1).toInt
    val stdDev = args(2).toDouble
    println("nbGroups " + nbGroups)
    println("stdDev " + stdDev)
    val nodesLabels = new IntIntHashMap
    val dedupEdges = new java.util.HashSet[(Int, Int)]
    def genGroup: Int = {
      Math.min(Math.abs(Random.nextGaussian() * stdDev).floor.toInt, nbGroups - 1) + 1
    }
    def getLabel(vId: Int): Int = {
      val l = nodesLabels.get(vId)
      if (l > 0) return l
      val assignedL = genGroup
      nodesLabels.put(vId, assignedL)
      return assignedL
    }
    var br = new BufferedReader(new FileReader(args(0)))
    val bw = new BufferedWriter(new FileWriter(args(0) + ".sami"))
    var line: String = null
    var currentSource = -1
    var nbEdges = 0
    do {
      line = br.readLine()
      if (line != null && !line.isEmpty()) {
        val sp = line.split(",")
        val vFrom = sp(0).toInt
        val vTo = sp(1).toInt
        if (!dedupEdges.contains((vTo, vFrom))) {
          nbEdges += 1
          dedupEdges.add((vFrom, vTo))
          if (vFrom != currentSource) {
            if (currentSource != -1) {
              bw.write("\n")
            }
            bw.write(vFrom + "\t" + getLabel(vFrom))
            currentSource = vFrom
          }
          bw.write("\t" + vTo + "\t" + getLabel(vTo) + "\t0")
        }
      }
    } while (line != null)
    br.close()
    bw.close()
    println("#edges: " + nbEdges)
  }
}