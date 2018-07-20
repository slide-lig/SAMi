package fr.liglab.sami

import fr.liglab.sami.code.DFSCode
import java.util.Comparator

abstract class PrioRunnable(val prio: DFSCode) extends Runnable {
}

class PrioComp extends Comparator[Runnable] {
  def compare(r1: Runnable, r2: Runnable): Int = {
    val pr1 = r1.asInstanceOf[PrioRunnable]
    val pr2 = r1.asInstanceOf[PrioRunnable]
    val lengthDiff = pr1.prio.codeLength() - pr2.prio.codeLength()
    if (lengthDiff != 0) {
      return lengthDiff
    } else {
      return pr1.prio.compare(pr2.prio)
    }
  }
}