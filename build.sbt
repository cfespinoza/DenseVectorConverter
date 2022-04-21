ThisBuild / version := "0.1.0"

ThisBuild / scalaVersion := "2.12.15"

lazy val root = (project in file("."))
  .settings(
    name := "DenseVectorConverter"
  )

val sparkVersion = "3.1.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion
)

// addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.6")