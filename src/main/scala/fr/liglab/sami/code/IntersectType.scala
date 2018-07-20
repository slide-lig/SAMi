package fr.liglab.sami.code

object IntersectType extends Enumeration {
  type IntersectType = Value
  val BBtoBB, BFtoBF, FFtoFF, FFtoFB, extend, self = Value
}