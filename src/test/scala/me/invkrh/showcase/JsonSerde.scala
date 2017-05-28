package me.invkrh.showcase

import org.json4s._
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.{read, write}

object JsonSerde {

  implicit val formats = Serialization.formats(NoTypeHints)

  def serialize(obj: AnyRef): String = {
    write(obj)
  }

  def deserialize[T: Manifest](jstr: String): T = {
    read[T](jstr)
  }
}
