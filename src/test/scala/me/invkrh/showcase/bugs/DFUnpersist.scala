package me.invkrh.showcase.bugs

import me.invkrh.showcase.SparkJobSpec

class DFUnpersist extends SparkJobSpec {
  import spark.implicits._

  "DataFrame.unpersist" can {
    "remove sub cached df" in {
      val a = Seq((1, 2), (2, 3)).toDF("a", "b").cache
      a.count

      val b = a.select('a, 'b + 1).cache
      b.count

      a.queryExecution.withCachedData
      b.queryExecution.withCachedData

      a.unpersist(true)
      // b should still be cached

      a.queryExecution.withCachedData
      b.queryExecution.withCachedData
    }
  }

  "RDD.unpersist" can {
    "keep derivative cached RDD" in {

      val aa =
        spark.sparkContext
          .makeRDD(Seq((1, 2), (2, 3)))
          .cache
          .setName("aa" + System.currentTimeMillis())
      aa.count

      val bb = aa.map { case (a, b) => (a, b + 1) }.cache.setName("bb" + System.currentTimeMillis())
      bb.count

      aa.unpersist(true)
    }
  }

}
