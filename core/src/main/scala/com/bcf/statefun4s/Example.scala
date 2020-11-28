package com.bcf.statefun4s

import scala.annotation.nowarn
import scala.concurrent.ExecutionContext

import cats.effect._
import cats.implicits._
import com.bcf.statefun4s.example.Example._
import org.http4s.server.blaze.BlazeServerBuilder

import StatefulFunction._

object Example extends IOApp {
  case class InputMsg(name: String)

  def greeterEntry[F[_]: StatefulFunction[*[_], Unit]: Sync](
      input: GreeterRequest
  ): F[Unit] =
    StatefulFunction[F, Unit].sendByteMessage("example", "greeter", input.name, input)

  def greeter[F[_]: StatefulFunction[*[_], GreeterState]: Sync](
      @nowarn input: GreeterRequest
  ): F[Unit] = {
    val statefun = StatefulFunction[F, GreeterState]
    for {
      newCount <- statefun.insideCtx(_.num + 1)
      _ <- statefun.modifyCtx(_.copy(newCount))
      _ <- Sync[F].delay(println(s"Saw $newCount time(s)"))
    } yield ()
  }

  override def run(args: List[String]): IO[ExitCode] = {
    val app = FunctionTable.makeApp(
      Map(
        ("example", "greeter") -> flinkWrapper(GreeterState(5))(
          protoInput(greeter[FunctionStack[IO, GreeterState, *]])
        ),
        ("example", "greeterEntry") -> flinkWrapper(())(
          protoInput(greeterEntry[FunctionStack[IO, Unit, *]])
        ),
      )
    )
    BlazeServerBuilder[IO](ExecutionContext.global)
      .bindHttp(
        8080,
        "0.0.0.0"
      )
      .withHttpApp(app)
      .serve
      .compile
      .drain
      .as(ExitCode.Success)
  }
}
