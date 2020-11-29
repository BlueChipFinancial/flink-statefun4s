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

trait StatefulFunction[F[_], S] {
  def getCtx: F[S]
  def setCtx(state: S): F[Unit]
  def insideCtx[A](inner: S => A): F[A]
  def modifyCtx(modify: S => S): F[Unit]
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

  implicit def stateFunStack[F[_]: Sync: Stateful[*[_], FunctionState[S]]: Raise[
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
