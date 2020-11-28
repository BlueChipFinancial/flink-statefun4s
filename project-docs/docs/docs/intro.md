---
layout: docs
title: Intro
permalink: docs/
---

# Monad Tracing

Provides tracing API's that can be used as a context bound on `F[_]`

```scala mdoc
import cats.effect._
import com.bcf.monadtracing._

def tracedFunction[F[_]: Trace: Sync]: F[Unit] =
  Trace[F].span("do_work") { Sync[F].delay(println("Doing work!")) }
```

Currently the only implementation of this typeclass is for `Kleisli[F, Span[F], *]` where `F[_]` is any
context that provides a `Bracket` instance (usually IO, ZIO, or Monix).

This allows for us to pass the `Span` down the callstack implicitly without each function having to keep
track of the spans and remembering to call `start()` and `finish()` on them.
