package com.bcf.statefun4s

import scala.annotation.nowarn
import scala.concurrent.ExecutionContext

import cats.effect._
import cats.implicits._
import com.bcf.statefun4s.example._
import com.bcf.statefun4s.generic._
import org.http4s.blaze.server.BlazeServerBuilder

import StatefulFunction._

@nowarn
object MacroExample extends IOApp {
  @FlinkFunction("greeter", "greeterEntry")
  @ProtoInput
  def greeterEntry[F[_]: StatefulFunction[*[_], Unit]: Sync](
      @FlinkMsg input: GreeterRequest
  ): F[Unit] = greeter.send(input.name, input)

  @FlinkFunction("greeter", "greeter")
  @ProtoInput
  def greeter[F[_]: StatefulFunction[*[_], GreeterState]: Sync](
      @FlinkMsg input: GreeterRequest
  ): F[Unit] = {
    val statefun = StatefulFunction[F, GreeterState]
    for {
      newCount <- statefun.insideCtx(_.num + 1)
      _ <- statefun.modifyCtx(_.copy(newCount))
      greeterResp = GreeterResponse(s"Saw ${input.name} $newCount time(s)")
      _ <- printer.send("singleton", greeterResp)
    } yield ()
  }

  @FlinkFunction("stdout", "printer")
  @ProtoInput
  def printer[F[_]: StatefulFunction[*[_], Unit]: Sync](
      @FlinkMsg input: GreeterResponse
  ): F[Unit] =
    Sync[F].delay(println(input))

  override def run(args: List[String]): IO[ExitCode] = {
    val app = FunctionTable.makeTypedApp(
      Map(
        greeter -> flinkWrapper(GreeterState(0))(
          greeter.serializedInput[FunctionStack[IO, GreeterState, *]]
        ),
        greeterEntry -> flinkWrapper(())(
          greeterEntry.serializedInput[FunctionStack[IO, Unit, *]]
        ),
        printer -> flinkWrapper(())(
          printer.serializedInput[FunctionStack[IO, Unit, *]]
        )
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
