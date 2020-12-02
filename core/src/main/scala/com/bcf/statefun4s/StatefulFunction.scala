package com.bcf.statefun4s

import scala.concurrent.duration.FiniteDuration

import cats.data._
import cats.effect.Sync
import cats.implicits._
import cats.mtl._
import cats.{Applicative, Monad}
import com.bcf.statefun4s.FlinkError.DeserializationError
import com.google.protobuf.{ByteString, any}
import org.apache.flink.statefun.flink.core.polyglot.generated.RequestReply.FromFunction.PersistedValueMutation
import org.apache.flink.statefun.flink.core.polyglot.generated.RequestReply.{
  Address,
  FromFunction,
  ToFunction
}
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
  def functionId: F[String]
  def sendMsg[A <: GeneratedMessage](
      namespace: String,
      fnType: String,
      id: String,
      data: A
  ): F[Unit]
  def sendDelayedMsg[A <: GeneratedMessage](
      namespace: String,
      fnType: String,
      id: String,
      delay: FiniteDuration,
      data: A
  ): F[Unit]
  def sendEgressMsg[A <: GeneratedMessage](
      namespace: String,
      fnType: String,
      data: A
  ): F[Unit]
  def sendByteMsg[A: Codec](namespace: String, fnType: String, id: String, data: A): F[Unit]
  def sendDelayedByteMsg[A: Codec](
      namespace: String,
      fnType: String,
      id: String,
      delay: FiniteDuration,
      data: A
  ): F[Unit]
}

object StatefulFunction {

  case class Env(functionNamespace: String, functionType: String, functionId: String)

  type FunctionStack[F[_], S, A] =
    EitherT[StateT[ReaderT[F, Env, *], FunctionState[S], *], FlinkError, A]

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
    val startState =
      input.state.headOption
        .map(_.stateValue.toByteArray())
        .filter(!_.isEmpty)
        .map(Codec[S].deserialize)
        .getOrElse(initialState.asRight)
        .leftMap(DeserializationError(_): FlinkError)
    val targetAddr =
      EitherT.fromOption[F](input.target, FlinkError.NoFunctionAddressGiven: FlinkError)
    targetAddr.flatMap { targetAddr =>
      val env = Env(targetAddr.namespace, targetAddr.`type`, targetAddr.id)
      input.invocations
        .foldLeft(EitherT.fromEither[F](startState.map(FunctionState(_)))) { (state, invocation) =>
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
        .map(stateToFromFunction[S])
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
          else Nil,
          state.invocations.toList,
          state.delayedInvocations.toList,
          state.egressMessages.toList
        )
      )
    )

  implicit def stateFunStack[F[_]: Sync: Ask[*[_], Env]: Stateful[*[_], FunctionState[S]]: Raise[
    *[_],
    FlinkError
  ], S: Codec]: StatefulFunction[F, S] =
    new StatefulFunction[F, S] {
      val stateful = Stateful[F, FunctionState[S]]
      override def getCtx: F[S] = stateful.inspect(_.ctx)
      override def setCtx(state: S): F[Unit] = stateful.modify(_.copy(ctx = state, mutated = true))
      override def insideCtx[A](inner: S => A): F[A] = stateful.inspect(fs => inner(fs.ctx))
      override def modifyCtx(modify: S => S): F[Unit] =
        stateful.modify(fs => fs.copy(ctx = modify(fs.ctx), mutated = true))
      override def functionId: F[String] = Ask[F, Env].reader(_.functionId)
      override def sendMsg[A <: GeneratedMessage](
          namespace: String,
          fnType: String,
          id: String,
          data: A
      ): F[Unit] =
        stateful.modify(fs =>
          fs.copy(
            invocations = fs.invocations :+ FromFunction.Invocation(
              Some(Address(namespace, fnType, id)),
              com.google.protobuf.any.Any.pack[A](data).some
            )
          )
        )
      override def sendDelayedByteMsg[A: Codec](
          namespace: String,
          fnType: String,
          id: String,
          delay: FiniteDuration,
          data: A
      ): F[Unit] =
        stateful.modify(fs =>
          fs.copy(
            delayedInvocations = fs.delayedInvocations :+ FromFunction.DelayedInvocation(
              delay.toMillis,
              Some(Address(namespace, fnType, id)),
              com.google.protobuf.any.Any("", ByteString.copyFrom(Codec[A].serialize(data))).some
            )
          )
        )
      override def sendDelayedMsg[A <: GeneratedMessage](
          namespace: String,
          fnType: String,
          id: String,
          delay: FiniteDuration,
          data: A
      ): F[Unit] =
        stateful.modify(fs =>
          fs.copy(
            delayedInvocations = fs.delayedInvocations :+ FromFunction.DelayedInvocation(
              delay.toMillis,
              Some(Address(namespace, fnType, id)),
              com.google.protobuf.any.Any.pack[A](data).some
            )
          )
        )
      override def sendEgressMsg[A <: GeneratedMessage](
          namespace: String,
          fnType: String,
          data: A
      ): F[Unit] =
        stateful.modify(fs =>
          fs.copy(
            egressMessages = fs.egressMessages :+ FromFunction.EgressMessage(
              namespace,
              fnType,
              com.google.protobuf.any.Any.pack[A](data).some
            )
          )
        )
      override def sendByteMsg[A: Codec](
          namespace: String,
          fnType: String,
          id: String,
          data: A
      ): F[Unit] =
        stateful.modify(fs =>
          fs.copy(
            invocations = fs.invocations :+ FromFunction.Invocation(
              Some(Address(namespace, fnType, id)),
              com.google.protobuf.any.Any("", ByteString.copyFrom(Codec[A].serialize(data))).some
            )
          )
        )
    }

  def apply[F[_]: StatefulFunction[*[_], S], S] = implicitly[StatefulFunction[F, S]]
}
