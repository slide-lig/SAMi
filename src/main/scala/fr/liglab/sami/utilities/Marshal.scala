package fr.liglab.sami.utilities

import net.openhft.koloboke.collect.set.hash._
import java.io._
import scala.reflect.ClassTag

object Marshal {
  import java.io._

  def writeSeqsToFile(filename: String, map: Map[Int, Seq[Int]]) {
    new PrintWriter(filename) {
      map.foreach {
        case (k, v) =>
          write(k + ":" + v)
          write("\n")
      }
      close()
    }
  }

  def dumpToFile[A](filename: String, obj: A)(implicit m: ClassTag[A]) {
    val out = new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream(filename)))
    out.writeObject(obj)
    out.close
  }

  def readFromFile[A](filename: String): A = {
    new ObjectInputStream(new BufferedInputStream(new FileInputStream(filename))).readObject().asInstanceOf[A]
  }

  //  def dump[A](o: A)(implicit m: ClassTag[A]): Array[Byte] = {
  //    val ba = new ByteArrayOutputStream(512)
  //    val out = new ObjectOutputStream(ba)
  //    out.writeObject(m)
  //    out.writeObject(o)
  //    out.close()
  //    ba.toByteArray()
  //  }

//  @throws(classOf[IOException])
//  @throws(classOf[ClassCastException])
//  @throws(classOf[ClassNotFoundException])
//  def load[A](buffer: Array[Byte])(implicit expected: ClassTag[A]): A = {
//    val in = new ObjectInputStream(new ByteArrayInputStream(buffer))
//    val found = in.readObject.asInstanceOf[ClassTag[_]]
//    if (found <:< expected) {
//      val o = in.readObject.asInstanceOf[A]
//      in.close()
//      o
//    } else {
//      in.close()
//      throw new ClassCastException("type mismatch;" +
//        "\n found   : " + found +
//        "\n required: " + expected)
//    }
//  }

}
