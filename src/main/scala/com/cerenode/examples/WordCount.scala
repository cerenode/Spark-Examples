package com.cerenode.examples

import com.cerenode.SparkJob
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

class MyUtil  {

  def counttwice(a:Int):Int = {
    a * 2
  }

  def twice(rdd: RDD[(String, Int)]): RDD[(String, Int)] = {
    rdd.map{ case (k,v) => (k , v * 2)}
  }
}


class CountTwice {
  val multiplier = 2

  def twice(rdd: RDD[(String, Int)]): RDD[(String, Int)] = {
    val temp = this.multiplier
    rdd.map{ case (k,v) => (k , v * temp)}
  }
}


class CountTwiceRDD(rdd: RDD[(String, Int)]) {
  val multiplier = 4

  def twice: RDD[(String, Int)] = {
    val s = this.multiplier
    rdd.map{ case (k,v) => (k , v * s)}
  }

}

object SparkWordCount extends  SparkJob {

  override def appName: String = "WordCount"

  override def run(spark: SparkSession, args:Array[String]): Unit = {
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val tokenized:RDD[String] = sc.textFile(args(0)).flatMap(_.split(" "))

    // count the occurrence of each word
    val wordCounts = tokenized.map((_, 1)).reduceByKey(_ + _)


    //val s = (new CountTwiceRDD(wordCounts)).twice

    //wordCounts.take(10).foreach(println(_))
   //

    val myUtil = new MyUtil
    //val s = wordCounts.map(x => (x._1, myUtil.counttwice(x._2) ))
    val s = myUtil.twice(wordCounts)
    s.take(10).foreach(println(_))
    sc.stop()

  }
}