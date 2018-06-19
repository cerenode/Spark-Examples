package com.cerenode.examples

import org.apache.spark.{ SparkContext, SparkConf}
import org.apache.spark.serializer.KryoRegistrator


class Util  extends   Serializable  {

  def double(a:Int):Int = {
    a * 2
  }

}


object Kryo {

  def main(args: Array[String]) {

    val sc = new SparkContext(new SparkConf().setAppName("Kryo")
      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      //.set("spark.kryo.registrationRequired", "true")
      .registerKryoClasses(Array(classOf[Util], classOf[Range]))

    )
    sc.setLogLevel("ERROR")


    val input = sc.parallelize(1 to 100)
    val myUtil = new Util
    val doubledRdd = input.map(x => myUtil.double(x))

    doubledRdd.collect().foreach(println(_))

    sc.stop()

  }
}


