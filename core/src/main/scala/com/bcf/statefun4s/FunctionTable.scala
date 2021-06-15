package com.bcf.statefun4s

import cats.data.EitherT
import cats.effect._
import cats.implicits._
import fs2.Chunk
import org.apache.flink.statefun.flink.core.polyglot.generated.RequestReply.{
  FromFunction,
  ToFunction
}
import org.http4s.dsl.Http4sDsl
import org.http4s.implicits._
import org.http4s.{
  DecodeFailure,
  DecodeResult,
  EntityDecoder,
  HttpApp,
  HttpRoutes,
  Media,
  MediaRange,
  Response,
  Status
}

import FlinkError._

object FunctionTable {
  type Table[F[_]] = Map[(String, String), ToFunction.InvocationBatchRequest => F[
    Either[FlinkError, FromFunction]
  ]]

  type DescriptorTable[F[_]] = Map[FunctionDescriptor, ToFunction.InvocationBatchRequest => F[
    Either[FlinkError, FromFunction]
  ]]

  def makeApp[F[_]: Sync](table: Table[F]): HttpApp[F] = makeRoutes(table).orNotFound
  def makeTypedApp[F[_]: Sync](table: DescriptorTable[F]): HttpApp[F] =
    makeRoutes(table.map { case (fd, f) => fd.namespaceType -> f }).orNotFound

  def makeRoutes[F[_]: Sync](table: Table[F]): HttpRoutes[F] =
    new Http4sDsl[F] {
      implicit val decoder: EntityDecoder[F, Array[Byte]] = new EntityDecoder[F, Array[Byte]] {
        override def decode(m: Media[F], strict: Boolean): DecodeResult[F, Array[Byte]] =
          DecodeResult(
            m.body.chunks.compile.toVector.map(bytes => Chunk.concat(bytes).asRight[DecodeFailure])
          ).map(_.toArray)
        override def consumes: Set[MediaRange] = Set(MediaRange.`*/*`)
      }

      def run: HttpRoutes[F] =
        HttpRoutes.of[F] {
          case req @ POST -> Root / "statefun" =>
            val result = for {
              body <- EitherT.liftF[F, FlinkError, Array[Byte]](req.as[Array[Byte]])
              toFunction <-
                Sync[F].delay(ToFunction.parseFrom(body)).attemptT.leftMap(DeserializationError(_))
              batch <- EitherT.fromOption[F](
                toFunction.request.invocation,
                ExpectedInvocationBatchRequest(toFunction.toProtoString): FlinkError
              )
              target <- EitherT.fromOption[F](batch.target, NoTargetInBatch: FlinkError)
              fn <- EitherT.fromOption[F](
                table.get((target.namespace, target.`type`)),
                NoSuchFunction(target.namespace, target.`type`): FlinkError
              )
              res <- EitherT(fn(batch))
            } yield res
            result.fold(
              err => Response(Status.BadRequest).withEntity(err.toString),
              success => Response(Status.Ok).withEntity(success.toByteArray)
            )
        }
    }.run
}
