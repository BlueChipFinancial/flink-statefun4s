package com.bcf.statefun4s

import scala.annotation.nowarn

import cats.effect._
import cats.implicits._
import com.bcf.statefun4s.example.Example._

import StatefulFunction._

object Main extends IOApp {
  case class InputMsg(name: String)

  def greeterEntry[F[_]: StatefulFunction[*[_], GreeterState]: Sync](
      input: GreeterRequest
  ): F[Unit] =
    StatefulFunction[F, GreeterState].sendByteMessage("example", "greeter", input.name, input)

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
    val output = greeter[FunctionStack[IO, GreeterState, *]](GreeterRequest("Tim"))
    for {
      (state, res) <-
        output.value.run(FunctionState(GreeterState(5))).run(Env("example", "greeter", "Tim"))
      _ <- IO(println(state))
    } yield ExitCode.Success
  }
}
