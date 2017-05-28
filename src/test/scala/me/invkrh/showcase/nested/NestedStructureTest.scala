package me.invkrh.showcase.nested

import scala.util.Random

import me.invkrh.showcase.{JsonSerde, SparkJobSpec}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.avg

object NestedStructureTest {
  object Position extends Enumeration { val DEV, OPS = Value }
  case class Person(name: String, age: Int, position: String, employer: Option[Employer])
  case class Employer(name: String, city: String)

  private val input = (0 to 10) map { i =>
    Person(
      "Hao" + i,
      20,
      Position.apply(i % 2).toString,
      if (Random.nextBoolean()) Some(Employer("criteo", "Paris" + i % 4)) else None
    )
  }

  private val ser = input.map(JsonSerde.serialize)
}

class NestedStructureTest extends SparkJobSpec {
  import spark.implicits._
  import NestedStructureTest._

  private val df = spark.sparkContext
    .makeRDD(ser)
    .map(p => JsonSerde.deserialize[Person](p))
    .toDF()

  def withJsonSerde(df: DataFrame): Unit = {
    df.printSchema()
    df.show(false)
  }

  "NestedStructure" can {
    "show the case" in {
      showCase("Nested Structure") {
        note("Input is a List of Person object")
        input foreach println
        note("Serialized to string, fields of Option.None are ignored")
        ser foreach println
        note("Converted to DataFrame with all fields for each row")
        df.show(false)
        note("GroupBy employer.name, if the nested field is null, the key will be null")
        val res = df
          .groupBy('employer getField "name" as "company_name")
          .agg(avg($"age"))
        res.show(false)
      }
    }
  }

}
