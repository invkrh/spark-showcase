package me.invkrh.showcase

import org.apache.spark.sql.SparkSession
import org.scalatest.WordSpec

trait SparkJobSpec extends WordSpec {
  protected lazy val spark: SparkSession = SparkSession
    .builder()
    .master("local")
    .appName("Spark SQL basic example")
    .getOrCreate()

  protected def title(t: String): Unit = {
    val lines = t.split("\n").map(_.trim)
    val maxLength = lines.map(_.length).max
    val content = lines.map(l => List(l + " " * (maxLength - l.length)).mkString("#  ", "", "  #"))

    println("#" * (maxLength + 6))
    content foreach println
    println("#" * (maxLength + 6))
    println
  }

  protected def note(t: String): Unit = {
    println
    println("### " + t)
    println
  }

  protected def showCase(titleText: String)(thunk: => Unit): Unit = {
    title(titleText)
    thunk
  }
}
