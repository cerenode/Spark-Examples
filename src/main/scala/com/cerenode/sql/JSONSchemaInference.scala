package com.cerenode.sql

import com.cerenode.SparkJob
import org.apache.spark.sql.SparkSession

object JSONSchemaInference extends SparkJob {

  override def appName: String = ""

  override def run(spark: SparkSession, args: Array[String]): Unit = {
    // easy case -- one record
    val ex1 = spark.read.json("public/inference1.json")
    ex1.schema.printTreeString()
    ex1.createOrReplaceTempView("table1")
    println("simple query")
    spark.sql("select b from table1").foreach(r => println(r))

    // two records, overlapping fields
    val ex2 = spark.read.json("public/inference2.json")
    ex2.schema.printTreeString()
    ex2.createOrReplaceTempView("table2")
    println("it's OK to reference a sometimes missing field")
    spark.sql("select b from table2").foreach(r => println(r))
    println("it's OK to reach into a sometimes-missing record")
    spark.sql("select g.h from table2").foreach(r => println(r))

    // two records, scalar and structural conflicts
    val ex3 = spark.read.json("public/inference3.json")
    ex3.schema.printTreeString()
    ex3.createOrReplaceTempView("table3")
    println("it's ok to query conflicting types but not reach inside them")
    // don't try to query g.h or g[1]
    spark.sql("select g from table3").foreach(r => println(r))

  }

}
