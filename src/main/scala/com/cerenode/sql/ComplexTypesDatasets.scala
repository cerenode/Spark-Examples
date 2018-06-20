package com.cerenode.sql

import com.cerenode.SparkJob
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ComplexTypesDatasets extends SparkJob {

  case class Point(x: Double, y: Double)

  case class Segment(from: Point, to: Point)

  case class Line(name: String, points: Array[Point])

  case class NamedPoints(name: String, points: Map[String, Point])

  case class NameAndMaybePoint(name: String, point: Option[Point])

  override def appName: String = "Complex Types in Datasets"

  override def run(spark: SparkSession, args: Array[String]): Unit = {

    import spark.implicits._

    //
    // Example 1: nested case classes
    //

    println("*** Example 1: nested case classes")

    val segments = Seq(
      Segment(Point(1.0, 2.0), Point(3.0, 4.0)),
      Segment(Point(8.0, 2.0), Point(3.0, 14.0)),
      Segment(Point(11.0, 2.0), Point(3.0, 24.0)))
    val segmentsDS = segments.toDS()

    segmentsDS.printSchema();

    // You can query using the field names of the case class as
    // as column names, ane we you descend down nested case classes
    // using getField() --
    // so the column from.x is specified using $"from".getField("x")
    println("*** filter by one column and fetch another")
    segmentsDS.where($"from".getField("x") > 7.0).select($"to").show()

    println("*** arrays")

    val lines = Seq(
      Line("a", Array(Point(0.0, 0.0), Point(2.0, 4.0))),
      Line("b", Array(Point(-1.0, 0.0))),
      Line("c", Array(Point(0.0, 0.0), Point(2.0, 6.0), Point(10.0, 100.0)))
    )
    val linesDS = lines.toDS()

    linesDS.printSchema()

    // notice here you can filter by the second element of the array, which
    // doesn't even exist in one of the rows
    println("*** filter by an array element")
    linesDS
      .where($"points".getItem(2).getField("y") > 7.0)
      .select($"name", size($"points").as("count")).show()

    println("*** maps")

    val namedPoints = Seq(
      NamedPoints("a", Map("p1" -> Point(0.0, 0.0))),
      NamedPoints("b", Map("p1" -> Point(0.0, 0.0),
        "p2" -> Point(2.0, 6.0), "p3" -> Point(10.0, 100.0)))
    )
    val namedPointsDS = namedPoints.toDS()

    namedPointsDS.printSchema()

    println("*** filter and select using map lookup")
    namedPointsDS
      .where(size($"points") > 1)
      .select($"name", size($"points").as("count"), $"points".getItem("p1")).show()


    println("*** Option")

    val maybePoints = Seq(
      NameAndMaybePoint("p1", None),
      NameAndMaybePoint("p2", Some(Point(-3.1, 99.99))),
      NameAndMaybePoint("p3", Some(Point(1.0, 2.0))),
      NameAndMaybePoint("p4", None)
    )
    val maybePointsDS = maybePoints.toDS()

    maybePointsDS.printSchema()

    println("*** filter by nullable column resulting from Option type")
    maybePointsDS
      .where($"point".getField("y") > 50.0)
      .select($"name", $"point").show()

    println("*** again its fine also to select through a column that's sometimes null")
    maybePointsDS.select($"point".getField("x")).show()

  }

}
