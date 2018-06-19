package com.cerenode.examples

import com.cerenode.SparkJob
import org.apache.spark.sql.SparkSession

object Pipe extends SparkJob{
  override def appName: String = "Pipe"

  override def run(spark: SparkSession, args: Array[String]): Unit = {
    val sc = spark.sparkContext
    val scriptPath = "/Users/gokulkg/echo.sh"

    val data = sc.parallelize(Seq("hi", "hello", "hey"))
    val pipeRDD = data.pipe(scriptPath)
    (pipeRDD.collect()).foreach(println(_))

    sc.stop()
  }

}
