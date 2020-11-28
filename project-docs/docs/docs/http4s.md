---
layout: docs
title: Http4s
permalink: docs/http4s/
---

# Http4s Client Middleware

An http4s client middleware is provided in the `monad-tracing-http4s-client` package.

## Blaze Client Usage

```scala mdoc
import cats.effect._
import cats.data._
import com.bcf.monadtracing._
import com.bcf.monadtracing.http4s.client.Middleware
import com.bcf.monadtracing.printing._
import org.http4s.client.Client
import org.http4s.client.blaze.BlazeClientBuilder
import scala.concurrent.ExecutionContext
import scala.annotation.nowarn

def putStrLn[F[_]: Sync](str: => String) = Sync[F].delay(println(str))

def runner[F[_]: Trace: Sync](@nowarn client: Client[F]): F[Unit] = {
  // Make HTTP requests with client
  // They will now include headers that pass
  // on the tracing context of the current Span
  Sync[F].unit
}

implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContext.global)


BlazeClientBuilder[IO](ExecutionContext.global).resource
  .parZip(PrintingTracer.rootSpan[IO](putStrLn[IO]))
  .use { case (client, tracer) =>
    val tracingClient: Client[Kleisli[IO, Span[IO], *]] = Middleware.liftKleisliMiddleware(client)
    tracer.root("MainFunction").use { span =>
      runner[Kleisli[IO, Span[IO], *]](tracingClient).run(span)
    }
  }.unsafeRunSync()
```

This creates a `Client[IO]` and lifts it into a `Client[Kleisli[IO, Span[IO], *]]` which gives the middleware access to the
current span so it can inject headers into all requests.