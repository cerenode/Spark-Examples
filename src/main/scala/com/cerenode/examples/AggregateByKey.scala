package com.cerenode.examples

import com.cerenode.SparkJob
import org.apache.spark.sql.SparkSession
import scala.collection.mutable

object AggregateByKey extends SparkJob{
  override def appName: String = "AggregateByKey"

  override def run(spark: SparkSession, args: Array[String]): Unit =  {
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    val keysWithValuesList = Array("foo=A", "foo=A", "foo=A", "foo=A", "foo=B", "bar=C", "bar=D", "bar=D")

    val data = sc.parallelize(keysWithValuesList)

    //Create key value pairs
    val kv = data.map(_.split("=")).map(v => (v(0), v(1))).cache()


    analyze(kv)




    val initialCount = 0;
    val addToCounts = (n: Int, v: String) => n + 1
    val sumPartitionCounts = (p1: Int, p2: Int) => p1 + p2

    val countByKey = kv.aggregateByKey(0)((n: Int, v: String) => n + 1, sumPartitionCounts)

    analyze(countByKey)


    val initialSet = mutable.HashSet.empty[String]
    val addToSet = (s: mutable.HashSet[String], v: String) => s += v
    val mergePartitionSets = (p1: mutable.HashSet[String], p2: mutable.HashSet[String]) => p1 ++= p2

    val uniqueByKey = kv.aggregateByKey(initialSet)(addToSet, mergePartitionSets)
    analyze(uniqueByKey)


  /*
    val initialFun = (s:String, v:String) => (s, 1)
    val acc = (s: String, v: String) => s += v
    val mergePartitionSetss = (p1: mutable.HashSet[String], p2: mutable.HashSet[String]) => p1 ++= p2

    val uniqueByKeys = kv.combineByKey(initialFun)(acc, mergePartitionSets)


    analyze(uniqueByKeys)
    */

    sc.stop()


  }

}
