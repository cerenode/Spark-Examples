package com.cerenode.examples

import com.cerenode.SparkJob
import org.apache.spark.{Partitioner, RangePartitioner}
import org.apache.spark.sql.SparkSession

import scala.collection.{Iterator, mutable}


object Partitions extends SparkJob {
  override def appName: String = "PartionsTest"

  override def run(spark: SparkSession, args:Array[String]): Unit = {


    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val numbers =  sc.parallelize(1 to 1000, 8)
    println("original RDD:")
    analyze(numbers)

    val some = numbers.filter(_ < 200)
    println("filtered RDD")
    analyze(some)


    val twoPartNoShuffle = some.coalesce(4)
    println("subset in two partitions without a shuffle")
    analyze(twoPartNoShuffle)
    println("it is a " + twoPartNoShuffle.getClass.getCanonicalName)



   val twoPart = some.coalesce(8, true)
   println("subset in two partitions after a shuffle")
   analyze(twoPart)
   println("it is a " + twoPart.getClass.getCanonicalName)





   // repartition
   val someRePartioned = some.repartition(10)
   println("numbers in someRePartioned")
   analyze(someRePartioned)
   println("it is a " + someRePartioned.getClass.getCanonicalName)





   // a ShuffledRDD  characteristics
   val groupedNumbers = numbers.groupBy(n => if (n % 2 == 0) "even" else "odd")
   println("numbers grouped into 'odd' and 'even'")
   analyze(groupedNumbers)
   println("it is a " + groupedNumbers.getClass.getCanonicalName)

   analyze(numbers.groupBy(n => if (n % 2 == 0) "even" else "odd"))
   //val finalRDD = numbers.mapPartitionsWithIndex((i,iter) => iter.map(n => if (n % 2 == 0) ("even_"+i, n)  else ("odd_"+i, n)))
   val finalRDD = numbers.mapPartitionsWithIndex((i,iter) => iter.map(n => if (n % 2 == 0) ("even_"+i, n)  else ("odd_"+i, n)))

   analyze(finalRDD)
   analyze(finalRDD.groupBy(_._1))

    sc.stop()

  }

}
