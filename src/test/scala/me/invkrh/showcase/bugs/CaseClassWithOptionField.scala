package me.invkrh.showcase.bugs

import me.invkrh.showcase.SparkJobSpec

case class A(a: Option[Double])
class CaseClassWithOptionField extends SparkJobSpec {
	import spark.implicits._
	val df = Seq(A(None), A(Some(2))).toDF
	df.show()
}
