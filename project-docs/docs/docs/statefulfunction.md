---
layout: docs
title: StatefulFunction
permalink: docs/statefulfunction/
---

# StatefulFunction

This typeclass provides access to all the functionality provided by Flink Stateful Functions.

All the methods it provides are based around managing state and sending messages.

## Working with State

`StatefulFunction` provides methods around accessing/mutating state. This state is committed in Flink and has ACID, exactly
once guarantees.

### Example

```scala mdoc
import cats.effect._, cats.implicits._
import com.bcf.statefun4s._
import scala.annotation.nowarn

case class GreeterRequest(name: String)
case class GreeterState(num: Int)

def greeter[F[_]: StatefulFunction[*[_], GreeterState]: Sync](
  @nowarn input: GreeterRequest
): F[Unit] = {
  val statefun = StatefulFunction[F, GreeterState]
  for {
    newCount <- statefun.insideCtx(_.num + 1)
    _ <- statefun.modifyCtx(_.copy(newCount))
  } yield ()
}
```

This `greeter` function accepts a message with a user's name and tied to the instance of that function
is some `GreeterState` instance.

So for example, if a consumer were to send message to `example/greeter` with the ID of `John`, it will
create a new instance of `GreeterState` for `John` and any future messages sent to that function ID will
use the current `GreeterState` for `John`. So if we sent one message and increment `num`, then send another message
with the same ID, Flink will give us the previous value of `num` for us to increment it. Since this state is committed
in lockstep with Kafka, we can safely increment this number without keeping a unique set of ids and counting them
idempotently.

## Sending Messages

Regular messages are sent to another function immediately. If the function ID sent to doesn't exist, Flink makes a new
inbox/state record for that function and passes the message.

### Example

With the following protobuf definitions:

```protobuf
message PrinterRequest {
  string msg = 1;
}
message GreeterRequest {
  string name = 1;
}
message GreeterState {
  int64 num = 1;
}
```

```scala
import cats.effect._, cats.implicits._
import com.bcf.statefun4s._

def greeter[F[_]: StatefulFunction[*[_], GreeterState]: Sync](
    input: GreeterRequest
): F[Unit] = {
  val statefun = StatefulFunction[F, GreeterState]
  for {
    newCount <- statefun.insideCtx(_.num + 1)
    _ <- statefun.modifyCtx(_.copy(newCount))
    _ <- statefun.sendMsg(
      "example",
      "printer",
      "universal",
      PrinterRequest(s"Saw ${input.name} ${newCount} time(s)")
    )
  } yield ()
}

def printer[F[_]: StatefulFunction[*[_], Unit]: Sync](
  input: PrinterRequest
): F[Unit] = Sync[F].delay(println(input.msg))
```

This will send the output of each counter increment to the printer and print it to stdout.
Note that in this case, printer is stateless so we use `Unit` as the state structure and pass some
arbitrary string like `"universal"`.

## Delayed messages

Delayed messages are stored in Flink state and scheduled to run at a different point in time. They do not require any events
flowing through Flink to trigger.

This is very useful for simulating windows or other more complex behavior.

### Example

```scala
import cats.effect._, cats.implicits._
import com.bcf.statefun4s._
import com.google.protobuf.any
import scala.concurrent.duration._
import java.util.UUID

sealed trait Cron
object Cron {
  final case class Create(cronString: String, event: any.Any) extends Cron
  final case object Trigger extends Cron
}

case class CronState(event: Option[Cron.Create] = None)

// Easy to imagine how this would be implemented
def nextRun(cronStr: String): FiniteDuration = ???

def cron[F[_]: StatefulFunction[*[_], CronState]: Sync](
    input: Cron
): F[Unit] = {
  val statefun = StatefulFunction[F, GreeterState]
  input match {
    case create @ Cron.Create(cronStr, _) =>
      for {
        _ <- statefun.modifyCtx(_.copy(Some(create)))
        id <- statefun.functionId
        _ <- statefun.sendDelayedMsg(
          "example",
          "cron",
          id,
          nextRun(cronStr)
        )
      } yield ()
    case Cron.Trigger =>
      for {
        id <- statefun.functionId
        state <- statefun.getCtx
        create <- Sync[F].fromOption(state.event, new RuntimeException("Event trigger was empty"))
        _ <- statefun.sendEgressMsg(
          "example",
          "triggers",
          KafkaProducerRecord(
            UUID.randomUUID().toString,
            create.event.toByteString,
            "triggers"
          )
        )
        _ <- statefun.sendDelayedMsg(
          "example",
          "cron",
          id,
          nextRun(create.cronStr)
        )
      } yield ()
  }
}
```

This is a basic idea for how one could implement a cron scheduler using delayed messages. Users who want to create a Cron
would send a message to `cron` with the ID of the job name (or some unique identifier), which would create some state to store
the event we are using `cron` to trigger on a schedule. The function will simply continuously sends itself delayed messages for
the next run time and publish the triggered event exactly once to Kafka.
