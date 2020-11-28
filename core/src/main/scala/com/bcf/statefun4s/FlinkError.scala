package com.bcf.statefun4s

sealed trait FlinkError
object FlinkError {
  final case class DeserializationError(err: Throwable) extends FlinkError
  final case object NoFunctionAddressGiven extends FlinkError
}
