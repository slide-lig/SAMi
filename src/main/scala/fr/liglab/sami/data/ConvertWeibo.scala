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
import org.eclipse.collections.impl.map.mutable.primitive.LongIntHashMap

object ConvertWeibo {

  def main(args: Array[String]): Unit = {
    val nodesNaming = new LongIntHashMap
    var br = new BufferedReader(new FileReader("uidlist.txt"))
    var line: String = null
    var pos = 0
    do {
      line = br.readLine()
      if (line != null) {
        if (!line.isEmpty()) {
          nodesNaming.put(line.toLong, pos)
        }
        pos += 1
      }
    } while (line != null)
    println("nodes naming for " + nodesNaming.size)
    br.close
    val nodesLabels = new IntIntHashMap
    val profileFiles = Array("userProfile/user_profile1.txt", "userProfile/user_profile2.txt")
    for (profileFile <- profileFiles) {
      val br = new BufferedReader(new FileReader(profileFile))
      var pos = 0
      var userId = -1L
      do {
        line = br.readLine()
        if (line != null) {
          //          println(line)
          if (!line.startsWith("#")) {
            if (line.isEmpty()) {
              pos = 0
              userId = -1
            } else {
              pos += 1
              if (pos == 1) {
                if (line.forall(_.isDigit)) {
                  userId = line.toLong
                } else {
                  userId = -1L
                }
              } else if (pos == 3 && userId != -1) {
                nodesLabels.put(nodesNaming.get(userId), line.toInt)
              }
            }
          }
        }
      } while (line != null)
      br.close
    }
    println("user labels for " + nodesLabels.size)
    nodesNaming.clear
    val dedupEdges = new java.util.HashSet[(Int, Int)]
    br = new BufferedReader(new FileReader("weibo_network.txt"))
    //skip first line
    br.readLine()
    val bw = new BufferedWriter(new FileWriter("weibo.sami"))
    var currentSource = -1
    var nbEdges = 0
    do {
      line = br.readLine()
      if (line != null && !line.isEmpty()) {
        val sp = line.split("\\s", 3)
        if (sp.size <= 2) {
          //          println("small line " + line)
        } else {
          val vId = sp(0).toInt
          if (nodesLabels.containsKey(vId)) {
            val vLbl = nodesLabels.get(vId)
            val spe = sp(2).split("\\s")
            var first = true
            for (i <- 0 until spe.length by 2) {
              val neighborId = spe(i).toInt
              if (nodesLabels.containsKey(neighborId)) {
                val edgeType = spe(i + 1).toInt
                if (edgeType == 0 || vId < neighborId) {
                  if (first) {
                    first = false
                    bw.write(vId + "\t" + vLbl)
                  }
                  val neighborLbl = nodesLabels.get(neighborId)
                  bw.write("\t" + neighborId + "\t" + neighborLbl + "\t0")
                  nbEdges += 1
                }
              }
            }
            if (!first) {
              bw.write("\n")
            }
          }
        }
      }
    } while (line != null)
    br.close()
    bw.close()
    println("#edges: " + nbEdges)
  }
}