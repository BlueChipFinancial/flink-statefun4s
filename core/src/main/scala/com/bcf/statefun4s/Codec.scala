package com.bcf.statefun4s

import simulacrum.typeclass

@typeclass trait Codec[A] {
  def serialize(data: A): Array[Byte]
  def deserialize(data: Array[Byte]): Either[Throwable, A]
}
