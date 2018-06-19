package com.cerenode

import org.apache.spark.sql.SparkSession

trait SparkJob {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName(appName)
      .master("local[*]")
      .getOrCreate()

     run(spark, args)
  }

  def run(spark: SparkSession, args: Array[String])

  def appName: String
}