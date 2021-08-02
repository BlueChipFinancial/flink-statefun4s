package com.bcf.statefun4s

import cats.implicits._
import org.apache.flink.statefun.sdk.reqreply.generated.RequestReply._

sealed abstract class FlinkError(
    msg: Option[String] = None,
    cause: Option[Throwable] = None
) extends Throwable(msg.getOrElse(cause.map(_.toString).orNull), cause.orNull)
object FlinkError {
  final case class DeserializationError(err: Throwable) extends FlinkError(cause = err.some)
  final case class BadTypeUrl(expected: String, actual: String)
      extends FlinkError(s"Expected typeUrl $expected but got $actual".some)
  final case class NoSuchFunction(namespace: String, fnType: String)
      extends FlinkError(s"Namespace: $namespace\n Function Type: $fnType".some)
  final case class ExpectedInvocationBatchRequest(received: String)
      extends FlinkError(s"Received $received".some)
  final case object NoFunctionAddressGiven
      extends FlinkError("No function address sent with request".some)
  final case object NoTargetInBatch
      extends FlinkError("Expected a target with the batch and received nothing".some)
  final case class ReplyWithNoCaller(callee: Address)
      extends FlinkError(s"Function: $callee tried to reply but had no caller".some)
  final case class NoCallerForFunction(callee: Address)
      extends FlinkError(s"Function: $callee tried to request a caller but had none".some)
}
