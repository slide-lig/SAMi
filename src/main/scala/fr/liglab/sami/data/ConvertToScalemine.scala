package fr.liglab.sami.data

import java.io.BufferedReader
import java.io.FileReader
import java.io.BufferedWriter
import java.io.FileWriter
import java.util.HashMap
import java.util.TreeMap

object ConvertToScalemine {
  def main(args: Array[String]): Unit = {
    println("starting conversion")
    val br = new BufferedReader(new FileReader(args(0)))
    val bwEdges = new BufferedWriter(new FileWriter(args(0) + ".edges"))
    val vertTypes = new TreeMap[Int, Int]()
    var line: String = null
    do {
      line = br.readLine()
      if (line != null) {
        val sp = line.split("\t", 3)
        if (sp.size <= 2) {
          return
        } else {
          val vId = sp(0).toInt
          val vLbl = sp(1).toInt
          val spe = sp(2).split('\t')
          vertTypes.put(vId, vLbl)
          for (i <- 0 until spe.length by 3) {
            val neighborId = spe(i).toInt
            val neighborLbl = spe(i + 1).toInt
            val edgeLbl = spe(i + 2).toInt
            vertTypes.put(neighborId, neighborLbl)
            bwEdges.write("e " + neighborId + " " + vId + " 1\n")
          }
        }
      }
    } while (line != null)
    br.close()
    bwEdges.close()
    val bwVertices = new BufferedWriter(new FileWriter(args(0) + ".vertices"))
    val iter = vertTypes.entrySet().iterator()
    while (iter.hasNext()) {
      val en = iter.next()
      bwVertices.write("v " + en.getKey + " " + en.getValue + "\n")
    }
    bwVertices.close()
  }
}