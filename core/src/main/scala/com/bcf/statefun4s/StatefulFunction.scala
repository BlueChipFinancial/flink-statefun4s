package com.bcf.statefun4s

import java.util.UUID

import scala.concurrent.duration.FiniteDuration

import cats._
import cats.data._
import cats.effect.Sync
import cats.implicits._
import cats.mtl._
import com.bcf.statefun4s.FlinkError.{
  BadTypeUrl,
  DeserializationError,
  NoCallerForFunction,
  ReplyWithNoCaller
}
import com.bcf.statefun4s.StatefulFunction.CancellationToken
import com.bcf.statefun4s.proto.sdkstate._
import com.google.protobuf.{ByteString, any}
import org.apache.flink.statefun.sdk.reqreply.generated.RequestReply.FromFunction.PersistedValueMutation
import org.apache.flink.statefun.sdk.reqreply.generated.RequestReply._

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
  def caller: F[Address]
  def callerOption: F[Option[Address]]
  def reply(data: any.Any): F[Unit]
  def selfMsg(data: any.Any): F[Unit]
  def selfDelayedMsg(delay: FiniteDuration, data: any.Any): F[Unit]
  def sendMsg(
      namespace: String,
      fnType: String,
      id: String,
      data: any.Any
  ): F[Unit]
  def sendMsg(
      addr: Address,
      data: any.Any
  ): F[Unit] = sendMsg(addr.namespace, addr.`type`, addr.id, data)
  def sendDelayedMsg(
      namespace: String,
      fnType: String,
      id: String,
      delay: FiniteDuration,
      data: any.Any
  ): F[CancellationToken]
  def sendEgressMsg(
      namespace: String,
      fnType: String,
      data: any.Any
  ): F[Unit]

  def cancelDelayed(
      namespace: String,
      fnType: String,
      id: String,
      clToken: CancellationToken
  ): F[Unit]

  def doOnce(fa: F[Unit]): F[Unit]
  def doOnceOrElse(fa: F[Unit])(fb: F[Unit]): F[Unit]
}

object StatefulFunction {

  case class Env(callee: Address, caller: Option[Address])

  sealed trait ExpirationMode
  final case object NONE extends ExpirationMode
  final case object AFTER_INVOKE extends ExpirationMode
  final case object AFTER_WRITE extends ExpirationMode

  case class Expiration(mode: ExpirationMode, after: FiniteDuration)

  case class CancellationToken(token: UUID) extends AnyVal

  type FunctionStack[F[_], S, A] =
    EitherT[StateT[ReaderT[F, Env, *], FunctionState[SdkState[S]], *], FlinkError, A]

  object FunctionStack {
    def pure[F[_]: Monad, S, A](a: A) =
      a.pure[EitherT[StateT[ReaderT[F, Env, *], FunctionState[SdkState[S]], *], FlinkError, *]]

    def liftK[F[_]: Monad, S]: F ~> FunctionStack[F, S, *] =
      EitherT
        .liftK[StateT[ReaderT[F, Env, *], FunctionState[SdkState[S]], *], FlinkError]
        .compose(
          StateT
            .liftK[ReaderT[F, Env, *], FunctionState[SdkState[S]]]
            .compose(ReaderT.liftK)
        )
  }

  def codecWrapper[F[_]: Monad, A: Codec, B](func: A => F[B])(implicit
      raise: Raise[F, FlinkError]
  ) = codecInputK[F, A].andThen(func).run

  def codecInputK[F[_]: Applicative, A: Codec](implicit
      raise: Raise[F, FlinkError]
  ) = Kleisli(codecInput[F, A])

  def codecInput[F[_]: Applicative, A: Codec](implicit
      raise: Raise[F, FlinkError]
  ): any.Any => F[A] = input => Codec[A].unpack(input).fold[F[A]](raise.raise, _.pure[F])

  def codecMapping[F[_]: Handle[
    *[_],
    FlinkError
  ]: Monad, A: Codec, B](
      mappers: (any.Any => F[A])*
  )(original: A => F[B]) =
    (codecInput[F, A] :: mappers.toList)
      .reduceLeftOption[any.Any => F[A]] {
        case (current, next) =>
          (a: any.Any) =>
            Handle[F, FlinkError].handleWith(current(a)) {
              case DeserializationError(_) | BadTypeUrl(_, _) => next(a)
              case otherwise                                  => Handle[F, FlinkError].raise(otherwise)
            }
      }
      .map(mapper => Kleisli(mapper).andThen(original).run)
      .getOrElse(codecWrapper(original))

  def flinkWrapper[F[_]: Monad, S: Codec](initialState: S, expiration: Option[Expiration] = None)(
      func: any.Any => FunctionStack[F, S, Unit]
  ): ToFunction.InvocationBatchRequest => F[Either[FlinkError, FromFunction]] = { input =>
    val mbPersistentValue =
      input.state
        .find(_.stateName == Constants.STATE_KEY)

    mbPersistentValue match {
      case None =>
        FromFunction(
          FromFunction.Response.IncompleteInvocationContext(
            FromFunction.IncompleteInvocationContext(
              List(
                FromFunction.PersistedValueSpec(
                  stateName = Constants.STATE_KEY,
                  typeTypename = Codec[S].typeUrl,
                  expirationSpec = expiration.map(e =>
                    FromFunction.ExpirationSpec(
                      mode = convertMode(e.mode),
                      expireAfterMillis = e.after.toMillis,
                    ),
                  ),
                )
              )
            )
          )
        ).asRight[FlinkError].pure[F]
      case Some(pv) =>
        val sdkState = pv.stateValue
          .map(_.toByteArray)
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
            .foldLeft(EitherT.fromEither[F](startState.map(FunctionState(_)))) {
              (state, invocation) =>
                val env = Env(targetAddr, invocation.caller)
                invocation.argument
                  .map { arg =>
                    state.flatMap { state =>
                      EitherT(func(typedValueToAny(arg)).value.run(state).run(env).map {
                        case (state, result) if state.deleted =>
                          result.map(_ => state.copy(ctx = SdkState(initialState, false)))
                        case (state, result) => result.map(_ => state)
                      })
                    }
                  }
                  .getOrElse(state)
            }
            .map { fs =>
              fs.map(sdkState =>
                SdkStateProto(
                  ByteString.copyFrom(Codec[S].serialize(sdkState.data)),
                  sdkState.doOnce
                )
              )
            }
            .map(stateToFromFunction(_))
        }.value
    }
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
                TypedValue(
                  Codec[S].typeUrl,
                  hasValue = true,
                  ByteString.copyFrom(Codec[S].serialize(state.ctx))
                ).some
              )
            )
          else if (state.deleted)
            List(
              PersistedValueMutation(
                PersistedValueMutation.MutationType.DELETE,
                Constants.STATE_KEY
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
  ], S]: StatefulFunction[F, S] =
    new StatefulFunction[F, S] {
      val stateful = Stateful[F, FunctionState[SdkState[S]]]
      override def getCtx: F[S] = stateful.inspect(_.ctx.data)

      override def setCtx(state: S): F[Unit] =
        stateful.modify(fs => fs.copy(ctx = fs.ctx.copy(data = state), mutated = true))

      override def insideCtx[A](inner: S => A): F[A] = stateful.inspect(fs => inner(fs.ctx.data))

      override def modifyCtx(modify: S => S): F[Unit] =
        stateful.modify(fs =>
          fs.copy(ctx = fs.ctx.copy(data = modify(fs.ctx.data)), mutated = true, deleted = false)
        )

      override def deleteCtx: F[Unit] = stateful.modify(_.copy(deleted = true, mutated = false))

      override def myAddr: F[Address] = Ask[F, Env].reader(_.callee)

      override def callerOption: F[Option[Address]] = Ask[F, Env].reader(_.caller)

      override def caller: F[Address] =
        for {
          callee <- myAddr
          callerOpt <- callerOption
          caller <-
            callerOpt
              .map(_.pure[F])
              .getOrElse(Raise[F, FlinkError].raise(NoCallerForFunction(callee)))
        } yield caller

      override def reply(data: com.google.protobuf.any.Any): F[Unit] =
        for {
          callee <- myAddr
          callerOpt <- callerOption
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
              anyToTypedValue(data).some
            )
          )
        )

      override def sendDelayedMsg(
          namespace: String,
          fnType: String,
          id: String,
          delay: FiniteDuration,
          data: com.google.protobuf.any.Any
      ): F[CancellationToken] =
        Sync[F]
          .delay(UUID.randomUUID())
          .flatMap(uuid =>
            stateful
              .modify(fs =>
                fs.copy(
                  delayedInvocations = fs.delayedInvocations :+ FromFunction.DelayedInvocation(
                    cancellationToken = uuid.toString,
                    delayInMs = delay.toMillis,
                    target = Address(namespace, fnType, id).some,
                    argument = anyToTypedValue(data).some
                  )
                )
              )
              .map(_ => CancellationToken(uuid))
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
              anyToTypedValue(data).some
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
        doOnceOrElse(fa)(Sync[F].unit)

      override def doOnceOrElse(fa: F[Unit])(fb: F[Unit]): F[Unit] =
        stateful
          .inspect(_.ctx.doOnce)
          .ifM(
            fb,
            fa *> stateful.modify(fs => fs.copy(ctx = fs.ctx.copy(doOnce = true), mutated = true))
          )

      override def cancelDelayed(
          namespace: String,
          fnType: String,
          id: String,
          clToken: CancellationToken
      ): F[Unit] =
        stateful.modify(fs =>
          fs.copy(
            delayedInvocations = fs.delayedInvocations :+ FromFunction.DelayedInvocation(
              isCancellationRequest = true,
              cancellationToken = clToken.token.toString,
              target = Address(namespace, fnType, id).some,
            )
          )
        )
    }

  private def anyToTypedValue(an: com.google.protobuf.any.Any): TypedValue =
    TypedValue(typename = an.typeUrl, hasValue = true, value = an.value)

  private def typedValueToAny(tv: TypedValue): com.google.protobuf.any.Any =
    com.google.protobuf.any.Any(typeUrl = tv.typename, value = tv.value)

  private def convertMode(mode: ExpirationMode): FromFunction.ExpirationSpec.ExpireMode =
    mode match {
      case NONE         => FromFunction.ExpirationSpec.ExpireMode.NONE
      case AFTER_INVOKE => FromFunction.ExpirationSpec.ExpireMode.AFTER_INVOKE
      case AFTER_WRITE  => FromFunction.ExpirationSpec.ExpireMode.AFTER_WRITE
    }

  def apply[F[_]: StatefulFunction[*[_], S], S] = implicitly[StatefulFunction[F, S]]
}
