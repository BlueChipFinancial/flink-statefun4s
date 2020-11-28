package com.bcf.statefun4s

import org.apache.flink.statefun.flink.core.polyglot.generated.RequestReply.ToFunction
import org.http4s.HttpApp
import org.http4s.HttpRoutes
import org.http4s.dsl.Http4sDsl
import org.http4s.implicits._
import cats._
import cats.effect._
import cats.implicits._

object FunctionTable {
  type Table[F[_]] = Map[(String, String), ToFunction.InvocationBatchRequest => F[Either[FlinkError, FunctionState[Array[Byte]]]]]

  def makeApp[F[_]: Sync](table: Table[F]): HttpRoutes[F] = new Http4sDsl[F] {
    def run: HttpRoutes[F] =
      HttpRoutes.of[F] {
        case req @ POST -> Root / "statefun" => for {
          body <- req.as[Array[Byte]]
        } yield ???
      }
  }.run
}