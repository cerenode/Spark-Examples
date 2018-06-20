package com.cerenode.sql

import java.io.File

import com.cerenode.SparkJob
import org.apache.spark.sql.SparkSession

object PartitionBySQL extends SparkJob {

  case class Fact(year: Integer, month: Integer, id: Integer, cat: Integer)

  override def appName: String = ""

  override def run(spark: SparkSession, args: Array[String]): Unit = {

    val exampleRoot = "public/PartitionBySQL"

    PartitionedTableHierarchy.deleteRecursively(new File(exampleRoot))

    val tableRoot = exampleRoot + "/Table"

    import spark.implicits._

    // create some sample data
    val ids = 1 to 1200
    val facts = ids.map(id => {
      val month = id % 12 + 1
      val year = 2000 + (month % 12)
      val cat = id % 4
      Fact(year, month, id, cat)
    })


    // make it an RDD and convert to a DataFrame
    val factsDF = spark.sparkContext.parallelize(facts, 4).toDF()

    println("*** Here is some of the sample data")
    factsDF.show(20)

    //
    // Register with a table name for SQL queries
    //
    factsDF.createOrReplaceTempView("original")

    spark.sql(
      s"""
         | DROP TABLE IF EXISTS partitioned
      """.stripMargin)

    //
    // Create the partitioned table, specifying the columns, the file format (Parquet),
    // the partitioning scheme, and where the directory hierarchy and files
    // will be located in the file system. Notice that Spark SQL allows you to
    // specify the partition columns twice, and doesn't require you to make them
    // the last columns.
    //
    spark.sql(
      s"""
         | CREATE TABLE partitioned
         |    (year INTEGER, month INTEGER, id INTEGER, cat INTEGER)
         | USING PARQUET
         | PARTITIONED BY (year, month)
         | LOCATION "$tableRoot"
      """.stripMargin)

    //
    // Now insert the sample data into the partitioned table, relying on
    // dynamic partitioning. Important: notice that the partition columns
    // must be provided last!
    //

    spark.sql(
      s"""
         | INSERT INTO TABLE partitioned
         | SELECT id, cat, year, month FROM original
      """.stripMargin)

    // print the resulting directory hierarchy with the Parquet files

    println("*** partitioned table in the file system, after the initial insert")
    PartitionedTableHierarchy.printRecursively(new File(tableRoot))

    // now we can query the partitioned table

    println("*** query summary of partitioned table")
    val fromPartitioned = spark.sql(
      s"""
         | SELECT year, COUNT(*) as count
         | FROM partitioned
         | GROUP BY year
         | ORDER BY year
      """.stripMargin)

    fromPartitioned.show()

    // dynamic partition insert -- no PARTITION clause

    spark.sql(
      s"""
         | INSERT INTO TABLE partitioned
         | VALUES
         |    (1400, 1, 2016, 1),
         |    (1401, 2, 2017, 3)
      """.stripMargin)

    // dynamic partition insert -- equivalent form with PARTITION clause

    spark.sql(
      s"""
         | INSERT INTO TABLE partitioned
         | PARTITION (year, month)
         | VALUES
         |    (1500, 1, 2016, 2),
         |    (1501, 2, 2017, 4)
      """.stripMargin)

    // static partition insert -- fully specify the partition using the
    // PARTITION clause

    spark.sql(
      s"""
         | INSERT INTO TABLE partitioned
         | PARTITION (year = 2017, month = 7)
         | VALUES
         |    (1600, 1),
         |    (1601, 2)
      """.stripMargin)

    // now for the mixed case -- in the PARTITION clause, 'year' is specified
    // statically and 'month' is dynamic

    spark.sql(
      s"""
         | INSERT INTO TABLE partitioned
         | PARTITION (year = 2017, month)
         | VALUES
         |    (1700, 1, 9),
         |    (1701, 2, 10)
      """.stripMargin)

    // check that all these inserted the data we expected

    println("*** the additional rows that were inserted")

    val afterInserts = spark.sql(
      s"""
         | SELECT year, month, id, cat
         | FROM partitioned
         | WHERE year > 2011
         | ORDER BY year, month
      """.stripMargin)

    afterInserts.show()

    println("*** partitioned table in the file system, after all additional inserts")
    PartitionedTableHierarchy.printRecursively(new File(tableRoot))

    println("*** query summary of partitioned table, after additional inserts")

    val finalCheck = spark.sql(
      s"""
         | SELECT year, COUNT(*) as count
         | FROM partitioned
         | GROUP BY year
         | ORDER BY year
      """.stripMargin)

    finalCheck.show()
  }

}
