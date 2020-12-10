package com.bcf.statefun4s

import cats.implicits._
import com.google.protobuf.any
import scalapb.{GeneratedMessage, GeneratedMessageCompanion}

case class ProtoCase[A <: GeneratedMessage: GeneratedMessageCompanion]() {
  def unapply(a: any.Any): Option[A] = Either.catchNonFatal(a.unpack[A]).toOption
}
