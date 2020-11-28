package com.bcf.statefun4s

import cats.data.EitherT
import cats.effect._
import cats.implicits._
import org.apache.flink.statefun.flink.core.polyglot.generated.RequestReply.{
  FromFunction,
  ToFunction
}
import org.http4s.dsl.Http4sDsl
import org.http4s.implicits._
import org.http4s.{HttpApp, HttpRoutes, Response, Status}

import FlinkError._

object FunctionTable {
  type Table[F[_]] = Map[(String, String), ToFunction.InvocationBatchRequest => F[
    Either[FlinkError, FromFunction]
  ]]

  def makeApp[F[_]: Sync](table: Table[F]): HttpApp[F] = makeRoutes(table).orNotFound

  def makeRoutes[F[_]: Sync](table: Table[F]): HttpRoutes[F] =
    new Http4sDsl[F] {
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
