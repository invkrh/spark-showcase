package me.invkrh.showcase.bugs

import me.invkrh.showcase.SparkJobSpec

case class Person(name: String, age: Int, childrenCnt: Int)

class InconsistentToDFFunc extends SparkJobSpec {
  import spark.implicits._
  val dataSeq = Seq(Person("Amy", 18, 0), Person("Bob", 30, 1))
  "ToDF" can {
    "lead to different schema" in {
      dataSeq.toDF.printSchema()
      spark.sparkContext.makeRDD(dataSeq).toDS.printSchema()
    }
  }
  // Bug resolved at 2.2.0
}
