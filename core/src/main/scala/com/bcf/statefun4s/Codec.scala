package com.bcf.statefun4s

import cats.implicits._
import io.circe.parser.decode
import io.circe.{Json, Codec => CirceCodec}
import scalapb.{GeneratedMessage, GeneratedMessageCompanion}
import simulacrum.typeclass

import scala.annotation.nowarn

@nowarn
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

  implicit def jsonCodec[A: CirceCodec]: Codec[A] =
    new Codec[A] {
      override def serialize(data: A): Array[Byte] = CirceCodec[A].apply(data).noSpaces.getBytes()
      override def deserialize(data: Array[Byte]): Either[Throwable, A] =
        Either
          .catchNonFatal(new String(data, "UTF-8"))
          .flatMap(decode[A])
    }

  implicit val jsonLitCodec: Codec[Json] = new Codec[Json] {
    override def serialize(data: Json): Array[Byte] = data.noSpaces.getBytes()
    override def deserialize(data: Array[Byte]): Either[Throwable, Json] =
      Either
        .catchNonFatal(new String(data, "UTF-8"))
        .flatMap(decode[Json])
  }

  implicit val unitCodec: Codec[Unit] = new Codec[Unit] {
    override def serialize(data: Unit): Array[Byte] = Array.empty
    override def deserialize(data: Array[Byte]): Either[Throwable, Unit] = ().asRight
  }

  implicit def optionCodec[A: Codec]: Codec[Option[A]] =
    new Codec[Option[A]] {
      override def serialize(data: Option[A]): Array[Byte] =
        data.map(Codec[A].serialize).getOrElse(Array.empty)
      override def deserialize(data: Array[Byte]): Either[Throwable, Option[A]] =
        if (data.isEmpty) None.asRight else Codec[A].deserialize(data).map(_.some)

    }
}
