package com.cerenode.examples

import com.cerenode.SparkJob
import org.apache.spark.sql.SparkSession

object CombineByKey extends SparkJob {
  override def appName: String = "CombineByKey"

  override def run(spark: SparkSession, args: Array[String]): Unit = {
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")



  }
}
