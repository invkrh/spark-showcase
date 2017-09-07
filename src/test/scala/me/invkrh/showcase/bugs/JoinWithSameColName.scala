package me.invkrh.showcase.bugs

import me.invkrh.showcase.SparkJobSpec
import org.apache.spark.sql.DataFrame

class JoinWithSameColName extends SparkJobSpec {
  import spark.implicits._

  def join(df1: DataFrame, df2: DataFrame): Unit = {
    df1
      .join(df2, Seq("key"), "inner")
      .select('key, df1("value"), df2("value"))
      .show
  }

  val dta = List((0, 1, 2), (0, 2, 4), (0, 3, 6))
  val dtb = List((1, 1, 1), (1, 2, 3), (1, 3, 5))

  val df = (dta ::: dtb).toDF("part", "key", "value")
  val ones = df.where('part === 1)
  val zeros = df.where('part === 0)

  val anotherZeros = dta.toDF("part", "key", "value")
  val anotherOnes = dtb.toDF("part", "key", "value")

  join(ones, zeros)
  join(anotherOnes, anotherZeros)
}
