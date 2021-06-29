package com.bcf.statefun4s.typed

import scala.annotation.implicitNotFound

@implicitNotFound("The calling function needs an input mapping for ${A}")
class CanAccept[A]
object CanAccept
