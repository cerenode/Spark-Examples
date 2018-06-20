package com.cerenode.sql

import com.cerenode.SparkJob
import org.apache.spark.sql.SparkSession

object JSON extends SparkJob {

  override def appName: String = "JSONs"

  override def run(spark: SparkSession, args: Array[String]): Unit = {

    import spark.implicits._

    // easy enough to query flat JSON
    val people = spark.read.json("public/flat.json")
    people.printSchema()
    people.createOrReplaceTempView("people")
    val young = spark.sql("SELECT firstName, lastName FROM people WHERE age < 30")
    young.foreach(r => println(r))

    // nested JSON results in fields that have compound names, like address.state
    val peopleAddr = spark.read.json("public/nonFlat.json")
    peopleAddr.printSchema()
    peopleAddr.foreach(r => println(r))
    peopleAddr.createOrReplaceTempView("peopleAddr")
    val inPA = spark.sql("SELECT firstName, lastName FROM peopleAddr WHERE address.state = 'PA'")
    inPA.foreach(r => println(r))

    // interesting characters in field names lead to problems with querying, as Spark SQL
    // has no quoting mechanism for identifiers
    val peopleAddrBad = spark.read.json("public/notFlatBadFieldName.json")
    peopleAddrBad.printSchema()

    // instead read the JSON in as an RDD[String], do necessary string
    // manipulations (example below is simplistic) and then turn it into a Schema RDD
    val lines = spark.read.textFile("public/notFlatBadFieldName.json")
    val linesFixed = lines.map(s => s.replaceAllLiterally("$", ""))
    val peopleAddrFixed = spark.read.json(linesFixed)
    peopleAddrFixed.printSchema()
    peopleAddrFixed.createOrReplaceTempView("peopleAddrFixed")
    val inPAFixed = spark.sql("SELECT firstName, lastName FROM peopleAddrFixed WHERE address.state = 'PA'")
    inPAFixed.foreach(r => println(r))

  }

}
