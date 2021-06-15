package com.bcf.statefun4s.generic

import scala.annotation.{StaticAnnotation, nowarn}
import scala.reflect.macros.whitebox

import cats.implicits._
import cats.mtl.Raise
import com.bcf.statefun4s.{FlinkError, FunctionDescriptor, StatefulFunction}

@nowarn("msg=never used")
class FlinkFunction(namespace: String, `type`: String) extends StaticAnnotation {
  def macroTransform(annottees: Any*): Any = macro FlinkFunctionImpl.impl
}

class FlinkMsg extends StaticAnnotation
class ProtoInput extends StaticAnnotation
class CodecInput extends StaticAnnotation

@nowarn("msg=never used")
object FlinkFunctionImpl {
  def impl(c: whitebox.Context)(annottees: c.Tree*) = {
    import c.universe._
    val (namespace, tpe) = c.prefix.tree match {
      case q"new FlinkFunction($namespace, $tpe)" =>
        (namespace, tpe)
      case _ =>
        c.abort(c.enclosingPosition, "FlinkFunction arguments didn't match what macro extracts")
    }
    val input = annottees.head
    val addition = input match {
      case func: DefDef =>
        val inputVar =
          func.vparamss
            .collectFirst(_.collectFirst {
              case field @ ValDef(Modifiers(_, _, annotations), _, _, _)
                  if annotations.exists(_ equalsStructure q"new FlinkMsg()") =>
                field
            })
            .flatten
        val higherKindedType =
          func.vparamss.flatten
            .collectFirst {
              case q"$_ val $_: StatefulFunction[$_, $_][$higherKindedType] = $_" =>
                higherKindedType.asInstanceOf[Ident]
              case q"$_ val $_: StatefulFunction[$higherKindedType, $_] = $_" =>
                higherKindedType.asInstanceOf[Ident]
              case q"$_ val $_: $_[$higherKindedType] = $_" =>
                higherKindedType.asInstanceOf[Ident]
            }

        (inputVar product higherKindedType)
          .map {
            case (input, higherKindedType) =>
              val flinkError = tq"${symbolOf[FlinkError.type].companion}"
              val raise = tq"${symbolOf[Raise.type].companion}"
              val statefulFunction = tq"${symbolOf[StatefulFunction.type].companion}"
              val functionDescriptor = tq"${symbolOf[FunctionDescriptor]}"
              val objName = TermName(func.name.toString)
              val duplicateParams = func.tparams.map { typeDef =>
                TypeDef(typeDef.mods, TypeName(c.freshName()), typeDef.tparams, typeDef.rhs)
              }
              val typeIdents = func.tparams.map(_.name).map(Ident(_))
              val nonInputParams = func.vparamss.map(_.filter(_ != input))
              val nonFlinkInputs = nonInputParams.init :+ nonInputParams.last ++
                List(
                  ValDef(
                    Modifiers(Flag.IMPLICIT | Flag.PARAM),
                    TermName(c.freshName()),
                    AppliedTypeTree(
                      raise,
                      List(higherKindedType, flinkError)
                    ),
                    EmptyTree
                  )
                )
              val msg = c.freshName()
              val msgValDef = ValDef(Modifiers(Flag.PARAM), TermName(msg), input.tpt, EmptyTree)
              val withFlinkInputs = func.vparamss.map(_.map {
                case `input`   => Ident(TermName(msg))
                case otherwise => Ident(otherwise.name)
              })
              def serializerGenerator(wrapper: Symbol): DefDef =
                q"""
                  def serializedInput[..${func.tparams}](...$nonFlinkInputs) = {
                    $wrapper(($msgValDef) => apply[..${typeIdents}](...$withFlinkInputs))
                  }
                  """
              val serializerSpecific = func.mods.annotations.collect {
                case q"new ProtoInput()" =>
                  val protoInput =
                    symbolOf[StatefulFunction.type].asClass.module.info
                      .member(TermName("protoInput"))
                  serializerGenerator(protoInput)
                case q"new CodecInput()" =>
                  val codecInput =
                    symbolOf[StatefulFunction.type].asClass.module.info
                      .member(TermName("byteInput"))
                  serializerGenerator(codecInput)
              }

              if (serializerSpecific.length > 1)
                c.abort(
                  c.enclosingPosition,
                  "One function cannot accept multiple serializers (e.g. Protobuf and Codec inputs cannot be used for the same function)"
                )

              val renameToApply =
                q"def apply[..${func.tparams}](...${func.vparamss}): ${func.tpt} = ${func.rhs}"

              val fName = TypeName(c.freshName())
              val stateName = TypeName(c.freshName())
              q"""
              object $objName extends $functionDescriptor {
                override val namespaceType = ($namespace, $tpe)
                def send[$fName[_], $stateName](id: String, msg: ${input.tpt})(implicit statefun: $statefulFunction[$fName, $stateName]) =
                  statefun.sendMsg($namespace, $tpe, id, msg)
                ..${serializerSpecific}
                ..${renameToApply}
              }
              """
          }
          .getOrElse(EmptyTree)
      case _ => EmptyTree
    }
    q"..$addition"
  }
}
