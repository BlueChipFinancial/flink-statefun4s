package com.bcf.statefun4s.typed

import scala.concurrent.duration.FiniteDuration

import com.bcf.statefun4s.StatefulFunction.CancellationToken
import com.bcf.statefun4s.{Codec, StatefulFunction}
import com.google.protobuf.any
import org.apache.flink.statefun.sdk.reqreply.generated.RequestReply.Address

trait TypedStatefulFunction[F[_], S, R] extends StatefulFunction[F, S] {
  def reply(reply: R): F[Unit]
  def reply(addr: Address, reply: R): F[Unit]
  def sendMsg(
      namespace: String,
      fnType: String,
      id: String,
      data: R
  ): F[Unit]
  def sendMsg(
      addr: Address,
      data: R
  ): F[Unit]
  def sendDelayedMsg(
      namespace: String,
      fnType: String,
      id: String,
      delay: FiniteDuration,
      data: R
  ): F[CancellationToken]
  def sendEgressMsg(
      namespace: String,
      fnType: String,
      data: R
  ): F[Unit]
  // def selfDelayedMsg(delay: FiniteDuration, data: R): F[Unit]
  // def selfMsg(data: R): F[Unit]
}

object TypedStatefulFunction {
  implicit def typedStatefulFunction[F[_], S, R: Codec](implicit
      statefun: StatefulFunction[F, S]
  ): TypedStatefulFunction[F, S, R] =
    new TypedStatefulFunction[F, S, R] {
      implicit private def r2any(data: R): any.Any = Codec[R].pack(data)
      override def getCtx: F[S] = statefun.getCtx
      override def setCtx(state: S): F[Unit] = statefun.setCtx(state)
      override def insideCtx[A](inner: S => A): F[A] = statefun.insideCtx(inner)
      override def modifyCtx(modify: S => S): F[Unit] = statefun.modifyCtx(modify)
      override def deleteCtx: F[Unit] = statefun.deleteCtx
      override def myAddr: F[Address] = statefun.myAddr
      override def caller: F[Address] = statefun.caller
      override def callerOption: F[Option[Address]] = statefun.callerOption
      override def reply(data: any.Any): F[Unit] = statefun.reply(data)
      override def selfMsg(data: any.Any): F[Unit] = statefun.selfMsg(data)
      override def selfDelayedMsg(delay: FiniteDuration, data: any.Any): F[Unit] =
        statefun.selfDelayedMsg(delay, data)
      override def sendMsg(namespace: String, fnType: String, id: String, data: any.Any): F[Unit] =
        statefun.sendMsg(namespace, fnType, id, data)
      override def sendDelayedMsg(
          namespace: String,
          fnType: String,
          id: String,
          delay: FiniteDuration,
          data: any.Any
      ): F[CancellationToken] = statefun.sendDelayedMsg(namespace, fnType, id, delay, data)
      override def sendEgressMsg(namespace: String, fnType: String, data: any.Any): F[Unit] =
        statefun.sendEgressMsg(namespace, fnType, data)
      override def doOnce(fa: F[Unit]): F[Unit] = statefun.doOnce(fa)
      override def doOnceOrElse(fa: F[Unit])(fb: F[Unit]): F[Unit] = statefun.doOnceOrElse(fa)(fb)
      override def reply(reply: R): F[Unit] =
        statefun.reply(reply)
      override def reply(addr: Address, reply: R): F[Unit] =
        statefun.sendMsg(addr, reply)
      override def sendMsg(namespace: String, fnType: String, id: String, data: R): F[Unit] =
        statefun.sendMsg(namespace, fnType, id, data)
      override def sendMsg(addr: Address, data: R): F[Unit] =
        statefun.sendMsg(addr, data)
      override def sendDelayedMsg(
          namespace: String,
          fnType: String,
          id: String,
          delay: FiniteDuration,
          data: R
      ): F[CancellationToken] = statefun.sendDelayedMsg(namespace, fnType, id, delay, data)
      override def sendEgressMsg(namespace: String, fnType: String, data: R): F[Unit] =
        statefun.sendEgressMsg(namespace, fnType, data)

      override def cancelDelayed(
          namespace: String,
          fnType: String,
          id: String,
          clToken: CancellationToken
      ): F[Unit] =
        statefun.cancelDelayed(namespace, fnType, id, clToken)
    }
}
