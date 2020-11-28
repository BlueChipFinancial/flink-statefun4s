package com.bcf.statefun4s

import cats.implicits._
import scalapb.{GeneratedMessage, GeneratedMessageCompanion}
import simulacrum.typeclass

@typeclass trait Codec[A] {
  def serialize(data: A): Array[Byte]
  def deserialize(data: Array[Byte]): Either[Throwable, A]
}

object Codec {
  implicit def inputCodec[A <: GeneratedMessage](implicit
      companion: GeneratedMessageCompanion[A]
  ): Codec[A] =
    new Codec[A] {
      override def serialize(data: A): Array[Byte] = data.toByteString.toByteArray()
      override def deserialize(data: Array[Byte]): Either[Throwable, A] =
        Either.catchNonFatal(companion.parseFrom(data))
    }

  implicit val unitCodec: Codec[Unit] = new Codec[Unit] {
    override def serialize(data: Unit): Array[Byte] = Array.empty
    override def deserialize(data: Array[Byte]): Either[Throwable, Unit] = ().asRight
  }
}
