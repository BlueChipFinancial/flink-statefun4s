---
layout: docs
title: Trace
permalink: docs/trace/
---

# Trace Typeclass

Used to create spans to wrap portions of code during which they are active.

## Usage

```scala mdoc
import cats.implicits._
import cats.data._
import cats.effect._
import com.bcf.monadtracing._
import com.bcf.monadtracing.printing._

def putStrLn[F[_]: Sync](str: => String) = Sync[F].delay(println(str))

def runner[F[_]: Trace: Sync](): F[Unit] = {
  Trace[F].span("First") {
    Trace[F].put("foo", "bar") *>
    Trace[F].span("Second") {
      putStrLn("Inside Second!")
    } *>
    putStrLn("Inside First!")
  } *>
  putStrLn("Outside All!")
}

PrintingTracer.rootSpan[IO](putStrLn[IO]).use { tracer =>
  tracer.root("MainFunction").use { span =>
    runner[Kleisli[IO, Span[IO], *]]().run(span)
  }
}.unsafeRunSync()
```

In this example, each `Trace[F].span` call creates a new `Span` for the duration of the effect passed as the
second argument list. When using the PrintingTracer it simply prints out the execution order of all the operations called on 
`Span`, but with real backends, like `monad-tracing-datadog`, it handles creation and cleanup of tracers and spans.
`Trace[F].put` allows attaching tags to spans that should be visible in whichever UI is used to view traces.

The `Kleilsli[IO, Span[IO], *]` might look big at first, but all that actually does is allow each `Trace[F]` function to access
the current `Span` at that stage in the callstack. Each function is essentially parameterized on the span and it actually gets passed in
using `.run(span)` at the top level. Each `Trace[F].span` call makes a function that takes the current span, creates a new child span, then
uses `Kleisli` composition to compose that function with the previous function offering the current span.

## Context

`Trace[F].context` returns the `Context` for the current span and can be used to propagate the `Span` remotely
to other application who will continue execution. For HTTP requests, this is just a `Map[String, String]` of headers
that the tracer injects to pass the `Context` to the next service.