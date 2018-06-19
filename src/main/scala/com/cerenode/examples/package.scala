package com.cerenode

import org.apache.spark.rdd.RDD

package object examples {
  def analyze[T](r: RDD[T]): Unit = {
    val partitions = r.glom()
    println(partitions.count() + " partitions")

    // use zipWithIndex() to see the index of each partition
    // we need to loop sequentially so we can see them in order: use collect()
    partitions.zipWithIndex().foreach {
      case (a, i) => {
        println("Partition " + i + " :> count : " + a.count(_ => true) + ") | " +
          "sample data >> " + a.take(10).foldLeft("")((e, s) => e + " " + s))
      }
    }
  }
}
