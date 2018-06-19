package com.cerenode.examples

import com.cerenode.SparkJob
import org.apache.spark.HashPartitioner
import org.apache.spark.sql.SparkSession

object Join extends SparkJob{

  override def appName: String = "Join"

  override def run(spark: SparkSession, args: Array[String]): Unit = {
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    val set1 =
      for ( x <- 1 to 100 ; z <- 'a' to 'z')
        yield (z , x)
    val set2 =
      for ( x <- 100 to 200 ; z <- 'a' to 'z')
        yield (z , x)

    val set1rdd = sc.parallelize(set1, 10)
    val partitionedset1 = set1rdd.partitionBy(new HashPartitioner(10))
    val set2rdd = sc.parallelize(set2, 10)
    val partitionedset2 = set2rdd.partitionBy(new HashPartitioner(10))

    analyze(set1rdd)
    analyze(set2rdd)
    analyze(set1rdd.join(set2rdd))



    analyze(partitionedset1)
    analyze(partitionedset2)
    analyze(partitionedset1.join(partitionedset2))






  }



}
