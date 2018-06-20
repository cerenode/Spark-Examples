package com.cerenode.sql

import com.cerenode.SparkJob
import org.apache.spark.sql.{Encoder, Encoders, SparkSession}
import org.apache.spark.sql.expressions.Aggregator

object TypedAggregation extends SparkJob{

  case class Employee(name: String, salary: Long)
  case class Average(sum: Long, count: Long)

  object MyAverage extends Aggregator[Employee, Average, Double] {
    // A zero value for this aggregation.
    def zero: Average = Average(0L, 0L)

    def reduce(buffer: Average, employee: Employee): Average = {
      Average(buffer.sum + employee.salary, buffer.count + 1)
    }
    // Merge two intermediate values
    def merge(b1: Average, b2: Average): Average = {
      Average(b1.sum + b2.sum, b1.count + b2.count)
    }
    // Transform the output of the reduction
    def finish(reduction: Average): Double = reduction.sum.toDouble / reduction.count
    // Specifies the Encoder for the intermediate value type
    def bufferEncoder: Encoder[Average] = Encoders.product
    // Specifies the Encoder for the final output value type
    def outputEncoder: Encoder[Double] = Encoders.scalaDouble
  }

  override def appName: String = "TypedAggregation"

  override def run(spark: SparkSession, args: Array[String]): Unit = {
    import spark.implicits._

    val ds = spark.read.json("public/employees.json").as[Employee]
    ds.show()

    val averageSalary = MyAverage.toColumn.name("average_salary")
    val result = ds.select(averageSalary)
    result.show()

    spark.stop()
  }

}