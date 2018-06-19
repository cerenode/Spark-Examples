package com.cerenode.examples

import com.cerenode.SparkJob
import org.apache.spark.sql.SparkSession


object JoinWithBroadcast extends SparkJob{
  override def appName: String = "Join with Braodcast"

  override def run(spark: SparkSession, args: Array[String]): Unit = {

   val sc = spark.sparkContext
   sc.setLogLevel("ERROR")

   val smallRDD = sc.parallelize(for ( id <- 1 to 100 ; z <- 'a' to 'z')
        yield (id, Person(id, "name_" + id +"_"+ z)))

   val largeRDD = sc.parallelize(for ( x <-1 to 4000) yield ( x , "something" ) )

    val smallLookup = sc.broadcast(smallRDD.collect.toMap)

    val res = largeRDD.flatMap { case(key, value) =>
      smallLookup.value.get(key).map { otherValue =>
        (key, (value, otherValue))
      }
    }

    analyze(res)


    val mediumRDD = sc.parallelize(for ( id <- 1 to 1000 ; z <- 'a' to 'z')
      yield (id, Person(id, "name_" + id +"_"+ z)))

    val keys = sc.broadcast(mediumRDD.map(_._1).collect.toSet)
    val reducedRDD = largeRDD.filter{ case(key, value) => keys.value.contains(key) }

    analyze(reducedRDD.join(mediumRDD))


    sc.stop()
  }
}
