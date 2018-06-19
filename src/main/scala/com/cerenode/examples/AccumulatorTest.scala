package com.cerenode.examples

import com.cerenode.SparkJob
import org.apache.spark.sql.SparkSession

object AccumulatorTest extends SparkJob {
  override def appName: String = "Accumulatortest"

  override def run(spark: SparkSession, args: Array[String]): Unit = {
    val sc = spark.sparkContext
    val words = sc.parallelize(Seq("name 1", "abc", "name 2", "name 4"))


    val names = new StringSetAccumulator
    sc.register(names)

    println("*** using a set accumulator")
    words.filter(_.startsWith("name")).foreach(names.add)

    println(names)
  }

}
