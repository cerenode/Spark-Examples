package com.cerenode.sql

import com.cerenode.SparkJob
import org.apache.spark.sql.SparkSession

object Dataset2 extends SparkJob {

  override def appName: String = "Datasets using case class"

  override def run(spark: SparkSession, args: Array[String]): Unit = {
    import spark.implicits._

    val numbers = Seq(
      Number(1, "one", "un"),
      Number(2, "two", "deux"),
      Number(3, "three", "trois"))
    val numberDS = numbers.toDS()

    println("*** case class Dataset types")
    numberDS.dtypes.foreach(println(_))

    // Since we used a case class we can query using the field names
    // as column names
    println("*** filter by one column and fetch another")
    numberDS.where($"i" > 2).select($"english", $"french").show()

    println("*** could have used SparkSession.createDataset() instead")
    val anotherDS = spark.createDataset(numbers)

    println("*** case class Dataset types")
    anotherDS.dtypes.foreach(println(_))
  }

}
