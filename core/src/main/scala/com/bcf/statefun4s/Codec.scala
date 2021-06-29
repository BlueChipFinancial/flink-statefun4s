package com.bcf.statefun4s

import scala.annotation.nowarn
import scala.reflect.ClassTag

import cats.implicits._
import com.bcf.statefun4s.FlinkError.BadTypeUrl
import com.google.protobuf.{ByteString, any}
import io.circe.parser.decode
import io.circe.{Codec => CirceCodec, Json}
import scalapb.{GeneratedMessage, GeneratedMessageCompanion}
import simulacrum.typeclass

@nowarn("msg=Unused import")
@typeclass trait Codec[A] {
  def serialize(data: A): Array[Byte]
  def deserialize(data: Array[Byte]): Either[FlinkError, A]
  def typeUrl: String
  def pack(data: A): any.Any =
    any.Any(typeUrl, ByteString.copyFrom(serialize(data)))
  def unpack(data: any.Any): Either[FlinkError, A] = {
    val url = typeUrl
    data match {
      case any.Any(`url`, value, _)  => deserialize(value.toByteArray())
      case any.Any(badTypeUrl, _, _) => (BadTypeUrl(url, badTypeUrl): FlinkError).asLeft
    }
  }
}

object Codec {
  implicit class CodecSyntax[A: Codec](data: A) {
    def pack = Codec[A].pack(data)
    def serialize = Codec[A].pack(data)
  }

  implicit def inputCodec[A <: GeneratedMessage](implicit
      companion: GeneratedMessageCompanion[A]
  ): Codec[A] =
    new Codec[A] {
      val url = any.Any.pack(companion.defaultInstance).typeUrl
      override def serialize(data: A): Array[Byte] = data.toByteString.toByteArray()
      override def deserialize(data: Array[Byte]): Either[FlinkError, A] =
        Either.catchNonFatal(companion.parseFrom(data)).leftMap(FlinkError.DeserializationError)
      override def typeUrl: String = url
    }

  implicit def jsonCodec[A: CirceCodec: ClassTag]: Codec[A] =
    new Codec[A] {
      val url = s"json/${implicitly[ClassTag[A]].runtimeClass.getName()}"
      override def serialize(data: A): Array[Byte] = CirceCodec[A].apply(data).noSpaces.getBytes()
      override def deserialize(data: Array[Byte]): Either[FlinkError, A] =
        Either
          .catchNonFatal(new String(data, "UTF-8"))
          .flatMap(decode[A])
          .leftMap(FlinkError.DeserializationError)
      override def typeUrl: String = url
    }

  implicit val jsonLitCodec: Codec[Json] = new Codec[Json] {
    val url = s"json/${Json.getClass.getName()}"
    override def serialize(data: Json): Array[Byte] = data.noSpaces.getBytes()
    override def deserialize(data: Array[Byte]): Either[FlinkError, Json] =
      Either
        .catchNonFatal(new String(data, "UTF-8"))
        .flatMap(decode[Json])
        .leftMap(FlinkError.DeserializationError)
    override def typeUrl: String = url
  }

  implicit val unitCodec: Codec[Unit] = new Codec[Unit] {
    override def serialize(data: Unit): Array[Byte] = Array.empty
    override def deserialize(data: Array[Byte]): Either[FlinkError, Unit] = ().asRight
    override def typeUrl: String = "statefun4s/Unit"
  }

  implicit def optionCodec[A: Codec]: Codec[Option[A]] =
    new Codec[Option[A]] {
      override def serialize(data: Option[A]): Array[Byte] =
        data.map(Codec[A].serialize).getOrElse(Array.empty)
      override def deserialize(data: Array[Byte]): Either[FlinkError, Option[A]] =
        if (data.isEmpty) None.asRight else Codec[A].deserialize(data).map(_.some)
      override def typeUrl: String = Codec[A].typeUrl
    }
}
