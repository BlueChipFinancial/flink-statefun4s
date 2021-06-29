package com.bcf.statefun4s

import scala.annotation.nowarn
import scala.concurrent.ExecutionContext

import cats.effect._
import cats.implicits._
import com.bcf.statefun4s.example.GreeterRpc.Empty
import com.bcf.statefun4s.example._
import com.bcf.statefun4s.generic._
import org.http4s.blaze.server.BlazeServerBuilder

import StatefulFunction._
import typed._

@nowarn
object MacroExample extends IOApp {
  @FlinkFunction("example", "greeterEntry")
  @MapInputs(
    (req: GreeterRequest) => GreeterEntryReq(req.name).asMessage,
    (resp: GreeterResponse) => resp.asMessage,
  )
  def greeterEntry[F[_]: StatefulFunction[*[_], Unit]: Sync](
      @FlinkMsg input: GreeterRpcMessage
  ): F[Unit] =
    input.toGreeterRpc match {
      case Empty                    => ().pure[F]
      case GreeterEntryReq(name, _) => greeter.ask[F, Unit](name, GreeterRequest(name, _))
      case resp: GreeterResponse    => printer.send("universal", resp)
    }

  @FlinkFunction("example", "greeter")
  def greeter[F[_]: Sync](
      @FlinkMsg input: GreeterRequest
  )(implicit statefun: TypedStatefulFunction[F, GreeterState, GreeterResponse]): F[Unit] = {
    val GreeterRequest(name, replyTo, _) = input
    for {
      newCount <- statefun.insideCtx(_.num + 1)
      _ <- statefun.modifyCtx(_.copy(newCount))
      greeterResp = GreeterResponse(s"Saw ${name} $newCount time(s)")
      _ <- statefun.sendMsg(replyTo, greeterResp)
    } yield ()
  }

  @FlinkFunction("example", "printer")
  def printer[F[_]: StatefulFunction[*[_], Unit]: Sync](
      @FlinkMsg input: GreeterResponse
  ): F[Unit] =
    Sync[F].blocking(println(input))

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
    IO(println("Starting up server for MacroExample")) *>
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
