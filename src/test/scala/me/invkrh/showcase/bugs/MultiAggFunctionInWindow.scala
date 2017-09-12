package me.invkrh.showcase.bugs

import me.invkrh.showcase.SparkJobSpec
import org.apache.spark.sql.DataFrame

class MultiAggFunctionInWindow extends SparkJobSpec {
  import spark.implicits._
  import org.apache.spark.sql.functions._
  import org.apache.spark.sql.expressions.Window

  val df = List((0, 1, 2), (0, 2, 4), (0, 3, 6), (1, 1, 1), (1, 2, 3), (1, 3, 5))
    .toDF("a", "b", "c")

  // Error
  // df.withColumn("value", sum('b) * sum('c) over Window.partitionBy('a)).show()

  df.withColumn("value", sum('b) over Window.partitionBy('a)).show()
  df.groupBy('a).agg(sum('b) * sum('c) as 'value).show
}
