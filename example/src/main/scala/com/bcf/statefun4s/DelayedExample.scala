package com.bcf.statefun4s

import cats.effect._
import cats.implicits._
import com.bcf.statefun4s.StatefulFunction.{FunctionStack, codecWrapper, flinkWrapper}
import com.bcf.statefun4s.example._
import org.http4s.blaze.server.BlazeServerBuilder

import scala.concurrent.ExecutionContext
import Codec._
import StatefulFunction._
import cats.Monad

import java.util.UUID
import scala.concurrent.duration._
import io.circe.generic.codec.DerivedAsObjectCodec.deriveCodec

object DelayedExample extends IOApp {

  case class NameCheck(name: String, cl: UUID)
  case class PrinterReq(name: String, times: Long)

  def greeterEntry[F[_]: StatefulFunction[*[_], Unit]](input: GreeterRequest): F[Unit] =
    StatefulFunction[F, Unit].sendMsg("example", "greeter", input.name, input.pack)

  def greeter[F[_]: StatefulFunction[*[_], GreeterState]: Monad](input: GreeterRequest): F[Unit] = {
    val statefun = StatefulFunction[F, GreeterState]
    for {
      newCount <- statefun.insideCtx(_.num + 1)
      _ <- statefun.modifyCtx(_.copy(newCount))
      clToken <- statefun.sendDelayedMsg("example", "printer", "pt", 5.seconds, PrinterReq(input.name, newCount).pack)
      _       <- statefun.sendMsg("example", "censor", "cs", NameCheck(input.name, clToken.token).pack)
    } yield ()
  }

  def printer[F[_]: Sync](req: PrinterReq): F[Unit] =
    Sync[F].delay(println(s"Saw ${req.name} ${req.times} times!"))

  val nonGrataList = List("George", "Kramer", "Elaine")

  def censor[F[_]: StatefulFunction[*[_], Unit]: Sync](req: NameCheck): F[Unit] = {
    val statefun = StatefulFunction[F, Unit]
    statefun.cancelDelayed(CancellationToken(req.cl)).whenA(nonGrataList.contains(req.name))
  }

  override def run(args: List[String]): IO[ExitCode] = {
    val app = FunctionTable.makeApp(
      Map(
        ("example", "greeter") -> flinkWrapper(GreeterState(0))(
          codecWrapper(greeter[FunctionStack[IO, GreeterState, *]])
        ),
        ("example", "greeterEntry") -> flinkWrapper(())(
          codecWrapper(greeterEntry[FunctionStack[IO, Unit, *]])
        ),
        ("example", "printer") -> flinkWrapper(())(
          codecWrapper(printer[FunctionStack[IO, Unit, *]])
        ),
        ("example", "censor") -> flinkWrapper(())(
          codecWrapper(censor[FunctionStack[IO, Unit, *]])
        ),
      )
    )
    IO(println("Starting up server")) *>
      BlazeServerBuilder[IO](ExecutionContext.global)
        .bindHttp(
          8080,
          "0.0.0.0"
        )
        // .withHttpApp(Logger[IO, IO](logHeaders = true, logBody = true, FunctionK.id)(app))
        .withHttpApp(app)
        .serve
        .compile
        .drain
        .as(ExitCode.Success)
  }
}
