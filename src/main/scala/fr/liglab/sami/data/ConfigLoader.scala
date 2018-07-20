package fr.liglab.sami.data

import scala.io.Source
import java.net.InetAddress


object ConfigLoader {
  def getNodeMap(nodeString: String): Map[String,Int] = {
    val nodeMap = nodeString.split(',').distinct.sorted.zipWithIndex.toMap
    //sanity check
    val localNodeAddress = InetAddress.getLocalHost().getHostAddress()
    if(!nodeMap.contains(localNodeAddress)){
      println("localNodeAddress: "+localNodeAddress)
      nodeMap.keys.foreach(println)
      throw new Exception("ConfigLoader misses on local node addr")
    }

    return nodeMap
  }
}
