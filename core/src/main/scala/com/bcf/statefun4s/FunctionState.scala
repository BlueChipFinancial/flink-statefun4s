package com.bcf.statefun4s

import cats._
import cats.data._
import cats.implicits._
import org.apache.flink.statefun.flink.core.polyglot.generated.RequestReply.FromFunction

case class FunctionState[A](
    ctx: A,
    mutated: Boolean = false,
    invocations: Chain[FromFunction.Invocation] = Chain.empty,
    functionStateDelayedInvocations: Chain[FromFunction.DelayedInvocation] = Chain.empty,
    functionStateEgressMessages: Chain[FromFunction.EgressMessage] = Chain.empty
)

object FunctionState {
  implicit val functorFunctionState: Functor[FunctionState] = new Functor[FunctionState] {
    override def map[A, B](fa: FunctionState[A])(f: A => B): FunctionState[B] = fa.copy(f(fa.ctx))
  }

  implicit val traverseFunctionState: Traverse[FunctionState] = new Traverse[FunctionState] {
    override def foldLeft[A, B](fa: FunctionState[A], b: B)(f: (B, A) => B): B = f(b, fa.ctx)
    override def foldRight[A, B](fa: FunctionState[A], lb: Eval[B])(
        f: (A, Eval[B]) => Eval[B]
    ): Eval[B] = f(fa.ctx, lb)
    override def traverse[G[_]: Applicative, A, B](fa: FunctionState[A])(
        f: A => G[B]
    ): G[FunctionState[B]] = f(fa.ctx).map(ctx => fa.copy(ctx = ctx))
  }
}
