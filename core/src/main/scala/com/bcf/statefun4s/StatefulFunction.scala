package com.bcf.statefun4s

import scala.concurrent.duration.FiniteDuration

import cats.data._
import cats.effect.Sync
import cats.implicits._
import cats.mtl._
import cats.{Applicative, Monad}
import com.bcf.statefun4s.FlinkError.{DeserializationError, ReplyWithNoCaller}
import com.bcf.statefun4s.proto.sdkstate._
import com.google.protobuf.{ByteString, any}
import org.apache.flink.statefun.flink.core.polyglot.generated.RequestReply.FromFunction.PersistedValueMutation
import org.apache.flink.statefun.flink.core.polyglot.generated.RequestReply._
import scalapb.{GeneratedMessage, GeneratedMessageCompanion}

/**
  * ==Overview==
  * Provides functions for interacting with Flink Statefun.
  *
  * [[getCtx]], [[setCtx]], [[insideCtx]], [[modifyCtx]] are all used for viewing/modifying
  * Flink state which is persistently stored in Flink and committed in lockstep
  * with Kafka consumer/producers.
  *
  * [[sendMsg]], [[sendDelayedMsg]] are used to pass messages from function to function
  * with or without some time delay. Delayed messages are stored in Flink state and scheduled
  * regardless of whether Flink has an event to react to.
  *
  * [[sendByteMsg]], etc. are used to send messages in non-protobuf format as just
  * pure bytes. Note that if you use this, the other SDK must support treating the protobuf
  * Any as just a byte container. Also if your function is accepted a non-protobuf message
  * you must use the [[StatefulFunction.byteInput]] rather than [[StatefulFunction.protoInput]] function.
  *
  * ==Greeter Example==
  * {{{
  *   case class GreeterRequest(name: String)
  *   case class GreeterState(num: Int)
  *
  *   def greeter[F[_]: StatefulFunction[*[_], GreeterState]: Sync](
  *      input: GreeterRequest
  *  ): F[Unit] = {
  *    val statefun = StatefulFunction[F, GreeterState]
  *    for {
  *      newCount <- statefun.insideCtx(_.num + 1)
  *      _ <- statefun.modifyCtx(_.copy(newCount))
  *      _ <- statefun.sendEgressMsg(
  *        "greeting",
  *        "greets",
  *        KafkaProducerRecord(
  *          input.name,
  *          GreeterResponse(s"Saw ${input.name} ${newCount} time(s)").toByteString,
  *          "greets"
  *        )
  *      )
  *    } yield ()
  *  }
  * }}}
  *
  * This function accepts a GreeterRequest with the persons name to greet in the message.
  * It must be called with the "id" being something unique to the user, which will cause the GreeterState
  * to be unique to the user and every user will have their own [[Int]] of the count.
  *
  * Flink calls out with a batch of events, the SDK will call this function for each event in the batch
  * while passing along the state from the previous call. Finally that state will be sent back to Flink
  * in a batch of mutations that are committed to Flink state with the Kafka offset ensuring exactly once processing.
  */
trait StatefulFunction[F[_], S] {
  def getCtx: F[S]
  def setCtx(state: S): F[Unit]
  def insideCtx[A](inner: S => A): F[A]
  def modifyCtx(modify: S => S): F[Unit]
  def deleteCtx: F[Unit]
  def myAddr: F[Address]
  def reply(data: any.Any): F[Unit]
  def reply[A <: GeneratedMessage](data: A): F[Unit] =
    reply(com.google.protobuf.any.Any.pack[A](data))
  def replyBytes[A: Codec](data: A): F[Unit] =
    reply(com.google.protobuf.any.Any("", ByteString.copyFrom(Codec[A].serialize(data))))

  def selfMsg(data: any.Any): F[Unit]
  def selfMsg[A <: GeneratedMessage](data: A): F[Unit] = selfMsg(any.Any.pack(data))
  def selfMsg[A: Codec](data: A): F[Unit] =
    selfMsg(any.Any("", ByteString.copyFrom(Codec[A].serialize(data))))
  def selfDelayedMsg(delay: FiniteDuration, data: any.Any): F[Unit]
  def selfDelayedMsg[A <: GeneratedMessage](delay: FiniteDuration, data: A): F[Unit] =
    selfDelayedMsg(delay, any.Any.pack(data))
  def selfDelayedMsg[A: Codec](delay: FiniteDuration, data: A): F[Unit] =
    selfDelayedMsg(delay, any.Any("", ByteString.copyFrom(Codec[A].serialize(data))))
  def sendMsg(
      namespace: String,
      fnType: String,
      id: String,
      data: any.Any
  ): F[Unit]
  def sendMsg[A <: GeneratedMessage](
      namespace: String,
      fnType: String,
      id: String,
      data: A
  ): F[Unit] = sendMsg(namespace, fnType, id, any.Any.pack(data))
  def sendByteMsg[A: Codec](namespace: String, fnType: String, id: String, data: A): F[Unit] =
    sendByteMsg(namespace, fnType, id, any.Any("", ByteString.copyFrom(Codec[A].serialize(data))))
  def sendDelayedMsg(
      namespace: String,
      fnType: String,
      id: String,
      delay: FiniteDuration,
      data: any.Any
  ): F[Unit]
  def sendDelayedMsg[A <: GeneratedMessage](
      namespace: String,
      fnType: String,
      id: String,
      delay: FiniteDuration,
      data: A
  ): F[Unit] = sendDelayedMsg(namespace, fnType, id, delay, any.Any.pack(data))
  def sendDelayedByteMsg[A: Codec](
      namespace: String,
      fnType: String,
      id: String,
      delay: FiniteDuration,
      data: A
  ): F[Unit] =
    sendDelayedMsg(
      namespace,
      fnType,
      id,
      delay,
      any.Any("", ByteString.copyFrom(Codec[A].serialize(data)))
    )
  def sendEgressMsg(
      namespace: String,
      fnType: String,
      data: any.Any
  ): F[Unit]
  def sendEgressMsg[A <: GeneratedMessage](
      namespace: String,
      fnType: String,
      data: A
  ): F[Unit] = sendEgressMsg(namespace, fnType, any.Any.pack(data))
  def sendEgressMsg[A: Codec](
      namespace: String,
      fnType: String,
      data: A
  ): F[Unit] =
    sendEgressMsg(namespace, fnType, any.Any("", ByteString.copyFrom(Codec[A].serialize(data))))

  def doOnce(fa: F[Unit]): F[Unit]
}

object StatefulFunction {

  case class Env(callee: Address, caller: Option[Address])

  type FunctionStack[F[_], S, A] =
    EitherT[StateT[ReaderT[F, Env, *], FunctionState[SdkState[S]], *], FlinkError, A]

  def protoInput[F[_]: Raise[
    *[_],
    FlinkError
  ]: Monad, A <: GeneratedMessage: GeneratedMessageCompanion, B](
      func: A => F[B]
  ): any.Any => F[B] = { input =>
    Raise[F, FlinkError]
      .catchNonFatal(input.unpack[A])(FlinkError.DeserializationError(_))
      .flatMap(func)
  }

  def byteInput[F[_]: Raise[
    *[_],
    FlinkError
  ]: Monad, A: Codec, B](
      func: A => F[B]
  ): any.Any => F[B] = { input =>
    Codec[A]
      .deserialize(input.value.toByteArray())
      .fold(
        err => Raise[F, FlinkError].raise(FlinkError.DeserializationError(err)),
        Applicative[F].pure
      )
      .flatMap(func)
  }

  def flinkWrapper[F[_]: Monad, S: Codec](initialState: S)(
      func: any.Any => FunctionStack[F, S, Unit]
  ): ToFunction.InvocationBatchRequest => F[Either[FlinkError, FromFunction]] = { input =>
    val sdkState =
      input.state
        .find(_.stateName == Constants.STATE_KEY)
        .map(_.stateValue.toByteArray())
        .filter(!_.isEmpty)
        .map(Codec[SdkStateProto].deserialize)
        .getOrElse(SdkStateProto(ByteString.EMPTY, false).asRight)

    val startState = sdkState
      .flatMap { sdkState =>
        val flinkState = sdkState.userState.toByteArray()
        val userState =
          if (flinkState.isEmpty)
            initialState.asRight
          else
            Codec[S].deserialize(flinkState)
        userState.map(SdkState(_, sdkState.doOnce))
      }
      .leftMap(DeserializationError(_): FlinkError)
    val targetAddr =
      EitherT.fromOption[F](input.target, FlinkError.NoFunctionAddressGiven: FlinkError)
    targetAddr.flatMap { targetAddr =>
      input.invocations
        .foldLeft(EitherT.fromEither[F](startState.map(FunctionState(_)))) { (state, invocation) =>
          val env = Env(targetAddr, invocation.caller)
          invocation.argument
            .map { arg =>
              state.flatMap { state =>
                EitherT(func(arg).value.run(state).run(env).map {
                  case (state, result) => result.map(_ => state)
                })
              }
            }
            .getOrElse(state)
        }
        .map { fs =>
          fs.map(sdkState =>
            SdkStateProto(ByteString.copyFrom(Codec[S].serialize(sdkState.data)), sdkState.doOnce)
          )
        }
        .map(stateToFromFunction(_))
    }.value

  }

  private def stateToFromFunction[S: Codec](state: FunctionState[S]): FromFunction =
    FromFunction(
      FromFunction.Response.InvocationResult(
        FromFunction.InvocationResponse(
          if (state.mutated)
            List(
              PersistedValueMutation(
                PersistedValueMutation.MutationType.MODIFY,
                Constants.STATE_KEY,
                ByteString.copyFrom(Codec[S].serialize(state.ctx))
              )
            )
          else if (state.deleted)
            List(
              PersistedValueMutation(
                PersistedValueMutation.MutationType.DELETE,
                Constants.STATE_KEY,
                ByteString.EMPTY
              )
            )
          else Nil,
          state.invocations.toList,
          state.delayedInvocations.toList,
          state.egressMessages.toList
        )
      )
    )

  implicit def stateFunStack[F[_]: Sync: Ask[*[_], Env]: Stateful[
    *[_],
    FunctionState[SdkState[S]]
  ]: Raise[
    *[_],
    FlinkError
  ], S: Codec]: StatefulFunction[F, S] =
    new StatefulFunction[F, S] {
      val stateful = Stateful[F, FunctionState[SdkState[S]]]
      override def getCtx: F[S] = stateful.inspect(_.ctx.data)
      override def setCtx(state: S): F[Unit] =
        stateful.modify(fs => fs.copy(ctx = fs.ctx.copy(data = state), mutated = true))
      override def insideCtx[A](inner: S => A): F[A] = stateful.inspect(fs => inner(fs.ctx.data))
      override def modifyCtx(modify: S => S): F[Unit] =
        stateful.modify(fs =>
          fs.copy(ctx = fs.ctx.copy(data = modify(fs.ctx.data)), mutated = true)
        )
      override def deleteCtx: F[Unit] = stateful.modify(_.copy(deleted = true))
      override def myAddr: F[Address] = Ask[F, Env].reader(_.callee)

      override def reply(data: com.google.protobuf.any.Any): F[Unit] =
        for {
          callee <- Ask[F, Env].reader(_.callee)
          callerOpt <- Ask[F, Env].reader(_.caller)
          caller <-
            callerOpt
              .map(_.pure[F])
              .getOrElse(Raise[F, FlinkError].raise(ReplyWithNoCaller(callee)))
          _ <- sendMsg(caller.namespace, caller.`type`, caller.id, data)
        } yield ()

      override def sendMsg(
          namespace: String,
          fnType: String,
          id: String,
          data: com.google.protobuf.any.Any
      ): F[Unit] =
        stateful.modify(fs =>
          fs.copy(
            invocations = fs.invocations :+ FromFunction.Invocation(
              Some(Address(namespace, fnType, id)),
              data.some
            )
          )
        )

      override def sendDelayedMsg(
          namespace: String,
          fnType: String,
          id: String,
          delay: FiniteDuration,
          data: com.google.protobuf.any.Any
      ): F[Unit] =
        stateful.modify(fs =>
          fs.copy(
            delayedInvocations = fs.delayedInvocations :+ FromFunction.DelayedInvocation(
              delay.toMillis,
              Some(Address(namespace, fnType, id)),
              data.some
            )
          )
        )

      override def sendEgressMsg(
          namespace: String,
          fnType: String,
          data: any.Any
      ): F[Unit] =
        stateful.modify(fs =>
          fs.copy(
            egressMessages = fs.egressMessages :+ FromFunction.EgressMessage(
              namespace,
              fnType,
              data.some
            )
          )
        )

      override def selfMsg(data: com.google.protobuf.any.Any): F[Unit] =
        for {
          addr <- myAddr
          _ <- sendMsg(addr.namespace, addr.`type`, addr.id, data)
        } yield ()

      override def selfDelayedMsg(
          delay: FiniteDuration,
          data: com.google.protobuf.any.Any
      ): F[Unit] =
        for {
          addr <- myAddr
          _ <- sendDelayedMsg(addr.namespace, addr.`type`, addr.id, delay, data)
        } yield ()

      override def doOnce(fa: F[Unit]): F[Unit] =
        stateful
          .inspect(_.ctx.doOnce)
          .ifM(
            Sync[F].unit,
            fa *> stateful.modify(fs => fs.copy(ctx = fs.ctx.copy(doOnce = true), mutated = true))
          )
    }

  def apply[F[_]: StatefulFunction[*[_], S], S] = implicitly[StatefulFunction[F, S]]
}
