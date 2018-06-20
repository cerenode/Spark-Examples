package com.cerenode.sql

import com.cerenode.SparkJob
import org.apache.spark.sql.SparkSession

object Basic2 extends SparkJob {

  override def appName: String = "Dataframes from RDD"

  override def run(spark: SparkSession, args: Array[String]): Unit = {
    import spark.implicits._

    // create an RDD of tuples with some data
    val custs = Seq(
      (1, "Widget Co", 120000.00, 0.00, "AZ"),
      (2, "Acme Widgets", 410500.00, 500.00, "CA"),
      (3, "Widgetry", 410500.00, 200.00, "CA"),
      (4, "Widgets R Us", 410500.00, 0.0, "CA"),
      (5, "Ye Olde Widgete", 500.00, 0.0, "MA")
    )
    val customerRows = spark.sparkContext.parallelize(custs, 4)

    // convert RDD of tuples to DataFrame by supplying column names
    val customerDF = customerRows.toDF("id", "name", "sales", "discount", "state")

    customerDF.printSchema()

    customerDF.show()
  }

}
