package fr.liglab.sami.code

import scala.collection.mutable.ListBuffer

import net.openhft.koloboke.collect.set.IntSet

abstract class DAG() extends DagProvider with DAGTrait {
  def get: DAG = this
  def isCanonical: Boolean = true
  def tryLock: Boolean = true
}

trait DAGTrait {
  def MNISupport: Int
  def leafSet: IntSet
  def paths: ListBuffer[Seq[Int]]
  def nbEmbeddings: Long
  def getNbEmbeddingStorageUnits: List[Int]
  def isEmpty: Boolean
  def hasEdgeFrom(v: Int): Boolean
  //  def extend(e: Map[Int, Seq[Int]], threshold:Int): DAG
  def extend(that: DAG, threshold: Int, lazyCleanup: Boolean, automorph: Array[List[Int]]): DAG = throw new UnsupportedOperationException()
  def intersectBBtoBB(that: DAG, threshold: Int, lazypCleanup: Boolean, automorph: Array[List[Int]]): DAG
  def intersectBFtoBF(that: DAG, threshold: Int, lazypCleanup: Boolean, automorph: Array[List[Int]]): DAG
  def intersectFFtoFF(that: DAG, threshold: Int, lazypCleanup: Boolean, automorph: Array[List[Int]], doubleFFPos: Int): DAG
  def intersectFFtoFB(that: DAG, threshold: Int, lazypCleanup: Boolean, automorph: Array[List[Int]], useProjection: Boolean): DAG
}
