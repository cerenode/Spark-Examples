name := "spark_examples"
version := "1.0"
organization := "com.cerenode"

scalaVersion := "2.11.11"
val sparkVersion = "2.2.0"

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
    "org.apache.spark" %% "spark-sql"  % sparkVersion % Provided,
    "org.scalatest"    %% "scalatest"  % "2.2.1"      % "test",
    "com.holdenkarau" %% "spark-testing-base" % "2.2.0_0.9.0" % "test"
)


