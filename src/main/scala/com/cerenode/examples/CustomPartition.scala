package com.cerenode.examples

import com.cerenode.SparkJob
import org.apache.spark.rdd.RDD
import org.apache.spark.Partitioner
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SparkSession


case class Person(id:Int, name:String)

class CustionPartitioner extends Partitioner {
  def numPartitions = 10

  def getPartition(key: Any) : Int = {
    key match {
      case (y:Int, z:String) => y % numPartitions

      case (p:Person) => (p.id % numPartitions).toInt

      case _ => throw new ClassCastException
    }
  }
}



object CustomPartition extends SparkJob{

  override def appName: String = "CustomPartitioner"

  override def run(spark: SparkSession, args: Array[String]): Unit = {

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val triplets =
      for (y <- 1 to 10; z <- 'a' to 'z')
        yield ((y, "name_"+z),  y)

    // Spark has the good sense to use the first tuple element
    // for range partitioning, but for this data-set it makes a mess
    val defaultRDD = sc.parallelize(triplets, 10)
    println("with default partitioning")
    analyze(defaultRDD)

    // out custom partitioner uses the second tuple element
    val partitionedRDD = defaultRDD.partitionBy(new CustionPartitioner())
    analyze(partitionedRDD)


    val pesrsons =
      for ( x <- 1 to 10 ; z <- 'a' to 'z')
        yield ( Person(x, "name_" +x +"_"+ z), x * 100)

    val pesrsonRDD = sc.parallelize(pesrsons, 10).partitionBy(new CustionPartitioner())

    analyze(pesrsonRDD)

    sc.stop()

  }
}

