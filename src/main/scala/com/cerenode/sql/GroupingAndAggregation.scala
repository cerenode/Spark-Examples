package com.cerenode.sql

import com.cerenode.SparkJob
import org.apache.spark.sql.{SparkSession, Column}
import org.apache.spark.sql.functions._

object GroupingAndAggregation extends SparkJob {

  override def appName: String = "Grouping and Aggregation"

  override def run(spark: SparkSession, args: Array[String]): Unit = {

    import spark.implicits._

    val custs = Seq(
      Cust(1, "Widget Co", 120000.00, 0.00, "AZ"),
      Cust(2, "Acme Widgets", 410500.00, 500.00, "CA"),
      Cust(3, "Widgetry", 410500.00, 200.00, "CA"),
      Cust(4, "Widgets R Us", 410500.00, 0.0, "CA"),
      Cust(5, "Ye Olde Widgete", 500.00, 0.0, "MA")
    )
    // make it an RDD and convert to a DataFrame
    val customerDF = spark.sparkContext.parallelize(custs, 4).toDF()

    // groupBy() produces a GroupedData, and you can't do much with
    // one of those other than aggregate it -- you can't even print it

    // basic form of aggregation assigns a function to
    // each non-grouped column -- you map each column you want
    // aggregated to the name of the aggregation function you want
    // to use
    //
    // automatically includes grouping columns in the DataFrame

    println("*** basic form of aggregation")
    customerDF.groupBy("state").agg("discount" -> "max").show()

    // you can turn of grouping columns using the SQL context's
    // configuration properties

    println("*** this time without grouping columns")
    spark.conf.set("spark.sql.retainGroupColumns", "false")
    customerDF.groupBy("state").agg("discount" -> "max").show()

    //
    // When you use $"somestring" to refer to column names, you use the
    // very flexible column-based version of aggregation, allowing you to make
    // full use of the DSL defined in org.apache.spark.sql.functions --
    // this version doesn't automatically include the grouping column
    // in the resulting DataFrame, so you have to add it yourself.
    //

    println("*** Column based aggregation")
    // you can use the Column object to specify aggregation
    customerDF.groupBy("state").agg(max($"discount")).show()

    println("*** Column based aggregation plus grouping columns")
    // but this approach will skip the grouped columns if you don't name them
    customerDF.groupBy("state").agg($"state", max($"discount")).show()

    // A simple user defined function
    def stddevFunc(c: Column): Column =
      sqrt(avg(c * c) - (avg(c) * avg(c)))

    println("*** A user-defined aggregation function")
    customerDF.groupBy("state").agg($"state", stddevFunc($"discount")).show()

    // there are some special short cuts on GroupedData to aggregate
    // all numeric columns
    println("*** Aggregation short cuts")
    customerDF.groupBy("state").count().show()
  }

}
