package fr.liglab.sami.utilities

import java.security.InvalidParameterException
import net.openhft.koloboke.function.IntObjConsumer
import net.openhft.koloboke.function.IntObjFunction
import net.openhft.koloboke.function.IntObjPredicate
import net.openhft.koloboke.collect.map.IntObjCursor
import net.openhft.koloboke.collect.map.IntObjMap
import net.openhft.koloboke.collect.set.IntSet
import net.openhft.koloboke.collect.IntCursor
import java.util.function.IntFunction
import java.util.function.BiFunction

class FakeMap[V >: Null](val content: IntSet, val uniqueValue: V) extends IntObjMap[V] {
  override def compute(key: Int, fun: IntObjFunction[_ >: V, _ <: V]) = throw new UnsupportedOperationException
  override def computeIfAbsent(key: Int, fun: IntFunction[_ <: V]) = throw new UnsupportedOperationException
  override def computeIfPresent(key: Int, fun: IntObjFunction[_ >: V, _ <: V]) = throw new UnsupportedOperationException
  override def containsKey(key: Int) = this.content.contains(key)
  override def containsKey(key: Object) = false
  override def entrySet = throw new UnsupportedOperationException
  override def forEach(f: IntObjConsumer[_ >: V]) = throw new UnsupportedOperationException
  override def forEachWhile(f: IntObjPredicate[_ >: V]) = throw new UnsupportedOperationException
  override def get(key: Int) = if (content.contains(key)) uniqueValue else null
  override def get(key: Object) = if (content.contains(key)) uniqueValue else null
  override def getOrDefault(key: Int, default: V) = if (content.contains(key)) uniqueValue else default
  override def getOrDefault(key: Object, default: V) = if (content.contains(key)) uniqueValue else default
  override def keySet = content
  override def merge(key: Int, value: V, f: BiFunction[_ >: V, _ >: V, _ <: V]) = throw new UnsupportedOperationException
  override def put(key: Int, value: V) = {
    if (value != uniqueValue) throw new InvalidParameterException("value must be unique with FakeMap")
    this.content.add(key)
    uniqueValue
  }
  override def put(key: Integer, value: V) = {
    if (value != uniqueValue) throw new InvalidParameterException("value must be unique with FakeMap")
    this.content.add(key.toInt)
    uniqueValue
  }
  override def putIfAbsent(key: Int, value: V) = {
    if (value != uniqueValue) throw new InvalidParameterException("value must be unique with FakeMap")
    this.content.add(key)
    uniqueValue
  }
  override def putIfAbsent(key: Integer, value: V) = {
    if (value != uniqueValue) throw new InvalidParameterException("value must be unique with FakeMap")
    this.content.add(key.toInt)
    uniqueValue
  }
  override def remove(key: Int) = {
    this.content.remove(key)
    uniqueValue
  }

  override def remove(key: Any) = {
    this.content.remove(key.asInstanceOf[Int])
    uniqueValue
  }

  override def remove(key: Int, value: Any) = {
    this.content.remove(key)
  }

  override def remove(key: Any, value: Any) = {
    this.content.remove(key.asInstanceOf[Int])
  }

  override def equals(that: Any): Boolean =
    that match {
      case that: FakeMap[V] => that.content.equals(this.content) && that.uniqueValue == this.uniqueValue
      case _                => false
    }

  override def removeIf(p: IntObjPredicate[_ >: V]) = throw new UnsupportedOperationException
  override def replace(key: Int, value: V) = throw new UnsupportedOperationException
  override def replace(key: Int, oldValue: V, newValue: V) = throw new UnsupportedOperationException
  override def replace(key: Integer, value: V) = throw new UnsupportedOperationException
  override def replace(key: Integer, oldValue: V, newValue: V) = throw new UnsupportedOperationException
  override def replaceAll(f: IntObjFunction[_ >: V, _ <: V]) = throw new UnsupportedOperationException
  override def valueEquivalence = throw new UnsupportedOperationException
  override def values = throw new UnsupportedOperationException
  override def clear = content.clear
  override def containsValue(value: Any) = uniqueValue.equals(value)
  override def hashCode = content.hashCode()
  override def isEmpty = content.isEmpty()
  override def ensureCapacity(minSize: Long) = content.ensureCapacity(minSize)
  override def shrink = content.shrink()
  override def putAll(m: java.util.Map[_ <: Integer, _ <: V]) = throw new UnsupportedOperationException
  override def size = content.size
  override def sizeAsLong = content.sizeAsLong
  override def cursor = new FakeCursor(content.cursor())
  override def toString(): String = {this.content.toString()}
  class FakeCursor(internal: IntCursor) extends IntObjCursor[V] {
    override def forEachForward(action: IntObjConsumer[_ >: V]) = throw new UnsupportedOperationException
    override def key = internal.elem()
    override def value = uniqueValue
    override def setValue(v: V) = throw new UnsupportedOperationException
    override def moveNext = internal.moveNext()
    override def remove = internal.remove()
  }

}