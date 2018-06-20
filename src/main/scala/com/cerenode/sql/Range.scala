package com.cerenode.sql

import com.cerenode.SparkJob
import org.apache.spark.sql.SparkSession

object Range extends SparkJob {

  override def appName: String = ""

  override def run(spark: SparkSession, args: Array[String]): Unit = {
    println("*** dense range with default partitioning")
    val df1 = spark.range(10, 14)
    df1.show()
    println("# Partitions = " + df1.rdd.partitions.length)

    println("\n*** stepped range")
    val df2 = spark.range(10, 14, 2)
    df2.show()
    println(df2.rdd.partitions.size)

    println("\n*** stepped range with specified partitioning")
    val df3 = spark.range(10, 14, 2, 2)
    df3.show()
    println("# Partitions = " + df3.rdd.partitions.length)

    println("\n*** range with just a limit")
    val df4 = spark.range(3)
    df4.show()
    println("# Partitions = " + df4.rdd.partitions.length)

  }

}
