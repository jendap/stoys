package io.stoys.spark

import java.util.{List => JList, Map => JMap}

import io.stoys.scala.Reflection

import scala.collection.JavaConverters._

object Py4j {
  def toArray[T](list: JList[T]): Array[T] = {
    list.toArray().asInstanceOf[Array[T]]
  }

  def toList[T](list: JList[T]): List[T] = {
    list.asScala.toList
  }

  def toMap[K, V](map: JMap[K, V]): Map[K, V] = {
    map.asScala.toMap
  }

  def toSeq[T](list: JList[T]): Seq[T] = {
    list.asScala
  }

  def toSet[T](list: JList[T]): Set[T] = {
    list.asScala.toSet
  }

  def copyCaseClass[T <: Product, V](originalValue: T, map: JMap[String, V]): T = {
    Reflection.copyCaseClass(originalValue, toMap(map))
  }
}
