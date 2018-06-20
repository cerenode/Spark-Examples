package com.cerenode.sql

import com.cerenode.SparkJob
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

object Basic3 extends SparkJob {

  override def appName: String = "Dataframes from Sequence of Rows"

  override def run(spark: SparkSession, args: Array[String]): Unit = {
    val custs = Seq(
      Row(1, "Widget Co", 120000.00, 0.00, "AZ"),
      Row(2, "Acme Widgets", 410500.00, 500.00, "CA"),
      Row(3, "Widgetry", 410500.00, 200.00, "CA"),
      Row(4, "Widgets R Us", 410500.00, 0.0, "CA"),
      Row(5, "Ye Olde Widgete", 500.00, 0.0, "MA")
    )
    val customerRows = spark.sparkContext.parallelize(custs, 4)

    val customerSchema = StructType(
      Seq(
        StructField("id", IntegerType, nullable = true),
        StructField("name", StringType, nullable = true),
        StructField("sales", DoubleType, nullable = true),
        StructField("discount", DoubleType, nullable = true),
        StructField("state", StringType, nullable = true)
      )
    )

    val customerDF = spark.createDataFrame(customerRows, customerSchema)

    customerDF.printSchema()

    customerDF.show()
  }

}
