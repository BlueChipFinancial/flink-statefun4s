package com.bcf.statefun4s

import java.nio.charset.StandardCharsets

import scala.concurrent.duration.DurationInt

import cats.effect.IO
import cats.syntax.option._
import com.bcf.statefun4s.StatefulFunction.{Env, FunctionStack}
import com.google.protobuf.ByteString
import org.apache.flink.statefun.sdk.egress.generated.Kafka.KafkaProducerRecord
import org.apache.flink.statefun.sdk.reqreply.generated.RequestReply.{
  Address,
  FromFunction,
  TypedValue
}
import weaver.{Expectations, SimpleIOSuite}

import Codec._

object StatefulFunctionSpec extends SimpleIOSuite {

  private val sf = StatefulFunction[FunctionStack[IO, Unit, *], Unit]
  private val selfAddress = Address("test", "reg", "tid0")
  private val defaultEnv = Env(selfAddress, None)
  private val testAddress = Address("test", "reg", "tid1")

  test("should send a message") {
    val fs = sf.sendMsg(testAddress, "some msg".pack)
    checkState(fs, (), defaultEnv) { state =>
      exists(state.invocations) { invc =>
        expect.same(
          FromFunction.Invocation(
            testAddress.some,
            TypedValue(
              typename = "test/string",
              hasValue = true,
              value = ByteString.copyFrom("some msg".getBytes(StandardCharsets.UTF_8))
            ).some
          ),
          invc
        ) and
          expect(state.delayedInvocations.isEmpty) and expect(state.egressMessages.isEmpty)
      }
    }
  }

  test("should send a delayed message") {
    val fs = sf.sendDelayedMsg(
      testAddress.namespace,
      testAddress.`type`,
      testAddress.id,
      3.seconds,
      "some msg".pack
    )
    checkState(fs, (), defaultEnv) { state =>
      exists(state.delayedInvocations) { invc =>
        expect.same(
          FromFunction.DelayedInvocation(
            delayInMs = 3000,
            target = testAddress.some,
            argument = TypedValue(
              typename = "test/string",
              hasValue = true,
              value = ByteString.copyFrom("some msg".getBytes(StandardCharsets.UTF_8))
            ).some
          ),
          invc
        )
      } and
        expect(state.invocations.isEmpty) and expect(state.egressMessages.isEmpty)
    }
  }

  test("should send an egress message") {
    val record = KafkaProducerRecord(
      key = "test-key",
      topic = "test-topic",
      valueBytes = ByteString.copyFrom("some msg".getBytes(StandardCharsets.UTF_8))
    )
    val fs = sf.sendEgressMsg("test-namespace", "test-egress-type", record.pack)
    checkState(fs, (), defaultEnv) { state =>
      exists(state.egressMessages) { invc =>
        expect.same(
          FromFunction.EgressMessage(
            egressNamespace = "test-namespace",
            egressType = "test-egress-type",
            argument = TypedValue(
              typename = record.pack.typeUrl,
              hasValue = true,
              value = record.pack.value
            ).some
          ),
          invc
        )
      } and
        expect(state.invocations.isEmpty) and expect(state.delayedInvocations.isEmpty)
    }
  }

  private def runM[A, S](fs: FunctionStack[IO, S, A], s0: S, env: Env) =
    fs.value.run(FunctionState(SdkState(s0, doOnce = false))).run(env)

  private def checkState[A, S](fs: FunctionStack[IO, S, A], s0: S, env: Env)(
      f: FunctionState[SdkState[S]] => Expectations
  ): IO[Expectations] = {
    val failF = fail("Failed to run function stack")
    runM[A, S](fs, s0, env).map {
      case (_, Left(err)) => failF(err)
      case (st, _)        => f(st)
    }
  }

  implicit private val cd: Codec[String] = new Codec[String] {
    override def serialize(data: String): Array[Byte] = data.getBytes(StandardCharsets.UTF_8)
    override def deserialize(data: Array[Byte]): Either[FlinkError, String] =
      Right(new String(data))
    override def typeUrl: String = "test/string"
  }
}
