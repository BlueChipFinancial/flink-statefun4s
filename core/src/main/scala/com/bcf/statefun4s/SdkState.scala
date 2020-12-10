package com.bcf.statefun4s

final case class SdkState[A](
    data: A,
    doOnce: Boolean
)
