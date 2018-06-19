package com.cerenode.examples
import scala.collection.mutable
import java.util.Collections

import org.apache.spark.util.AccumulatorV2

import scala.collection.JavaConversions._

class StringSetAccumulator extends AccumulatorV2[String, java.util.Set[String]] {
  private val _set = Collections.synchronizedSet(new java.util.HashSet[String]())
  override def isZero: Boolean = _set.isEmpty
  override def copy(): AccumulatorV2[String, java.util.Set[String]] = {
    val newAcc = new StringSetAccumulator()
    newAcc._set.addAll(_set)
    newAcc
  }
  override def reset(): Unit = _set.clear()
  override def add(v: String): Unit = _set.add(v)

  override def merge(other: AccumulatorV2[String, java.util.Set[String]]): Unit = {
    _set.addAll(other.value)
  }
  override def value: java.util.Set[String] = _set
}
