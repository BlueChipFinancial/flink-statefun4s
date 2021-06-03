package com.bcf.statefun4s

import scala.concurrent.ExecutionContext
import scala.jdk.DurationConverters._
import cats.effect._
import cats.implicits._
import com.bcf.statefun4s.example._
import com.google.protobuf.any
import com.google.protobuf.duration.Duration
import org.apache.flink.statefun.flink.core.polyglot.generated.RequestReply.Address
import org.apache.flink.statefun.flink.io.generated.Kafka.KafkaProducerRecord
import StatefulFunction._
import org.http4s.blaze.server.BlazeServerBuilder

import scala.annotation.nowarn

@nowarn
object Example extends IOApp {
  def greeterEntry[F[_]: StatefulFunction[*[_], Unit]: Sync](
      input: GreeterRequest
  ): F[Unit] =
    StatefulFunction[F, Unit].sendMsg("example", "greeter", input.name, input)

  def greeter[F[_]: StatefulFunction[*[_], GreeterState]: Sync](
      input: GreeterRequest
  ): F[Unit] = {
    val statefun = StatefulFunction[F, GreeterState]
    for {
      newCount <- statefun.insideCtx(_.num + 1)
      _ <- statefun.modifyCtx(_.copy(newCount))
      greeterResp = GreeterResponse(s"Saw ${input.name} ${newCount} time(s)")
      _ <- statefun.sendEgressMsg(
        "greeting",
        "greets",
        KafkaProducerRecord(
          input.name,
          greeterResp.toByteString,
          "greets"
        )
      )
      addr <- statefun.myAddr
      _ <- statefun.doOnce {
        statefun.sendMsg(
          "util",
          "batch",
          addr.id,
          BatchConfig(Address("example", "printer", "universal").some, Duration(5).some)
        )
      }
      _ <- statefun.sendMsg("util", "batch", addr.id, greeterResp)
    } yield ()
  }

  def printer[F[_]: StatefulFunction[*[_], Unit]: Sync](
      input: BatchOutput
  ): F[Unit] =
    Sync[F].delay(println("Got batch:")) *>
      input.msg.toList
        .traverse { msg =>
          Sync[F].delay(msg.unpack[GreeterResponse].toString())
        }
        .flatMap { msgs =>
          Sync[F].delay(println(msgs))
        } *>
      Sync[F].delay(println("Finished batch"))

  def batch[F[_]: StatefulFunction[*[_], BatchState]: Sync](
      input: any.Any
  ): F[Unit] = {
    val statefun = StatefulFunction[F, BatchState]
    val batchFinished = new ProtoCase[BatchFinished]()
    val batchConfig = new ProtoCase[BatchConfig]()
    input match {
      case batchConfig(conf) =>
        for {
          _ <- conf.addr.traverse_(addr => statefun.modifyCtx(_.copy(addr = addr)))
          _ <- conf.window.traverse_(window => statefun.modifyCtx(_.copy(window = window)))
        } yield ()
      case batchFinished(_) =>
        for {
          msgs <- statefun.insideCtx(_.msg)
          (addr, window) <- statefun.insideCtx(ctx => (ctx.addr, ctx.window))
          _ <- statefun.sendMsg(addr.namespace, addr.`type`, addr.id, BatchOutput(msgs))
          _ <- statefun.selfDelayedMsg(window.asJavaDuration.toScala, BatchFinished())
          _ <- statefun.modifyCtx(_.copy(msg = Seq.empty))
        } yield ()
      case msg =>
        statefun.doOnce {
          for {
            id <- statefun.myAddr.map(_.id)
            window <- statefun.insideCtx(_.window.asJavaDuration.toScala)
            _ <- Sync[F].delay(println(s"Scheduled batch for $window from now for $id"))
            _ <- statefun.selfDelayedMsg(window, BatchFinished())
          } yield ()
        } *>
          statefun.modifyCtx(ctx => ctx.copy(msg = ctx.msg :+ msg))
    }
  }

  override def run(args: List[String]): IO[ExitCode] = {
    val app = FunctionTable.makeApp(
      Map(
        ("example", "greeter") -> flinkWrapper(GreeterState(0))(
          protoInput(greeter[FunctionStack[IO, GreeterState, *]])
        ),
        ("example", "greeterEntry") -> flinkWrapper(())(
          protoInput(greeterEntry[FunctionStack[IO, Unit, *]])
        ),
        ("example", "printer") -> flinkWrapper(())(
          protoInput(printer[FunctionStack[IO, Unit, *]])
        ),
        ("util", "batch") -> flinkWrapper(
          BatchState(Duration(10), Address("example", "printer", "universal"))
        )(
          batch[FunctionStack[IO, BatchState, *]]
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
