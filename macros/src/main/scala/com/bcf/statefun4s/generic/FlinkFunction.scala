package com.bcf.statefun4s.generic

import scala.annotation.{StaticAnnotation, nowarn}
import scala.reflect.macros.whitebox

import cats.Monad
import cats.implicits._
import cats.mtl.Handle
import com.bcf.statefun4s.typed.{CanAccept, TypedStatefulFunction}
import com.bcf.statefun4s.{Codec, FlinkError, FunctionDescriptor, StatefulFunction}
import org.apache.flink.statefun.flink.core.polyglot.generated.RequestReply.Address

@nowarn("msg=never used")
class FlinkFunction(namespace: String, `type`: String) extends StaticAnnotation {
  def macroTransform(annottees: Any*): Any = macro FlinkFunctionImpl.impl
}

@nowarn("msg=never used")
class MapInputs(func: (Any => Any)*) extends StaticAnnotation
class FlinkMsg extends StaticAnnotation

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
    val out = input match {
      case func: DefDef =>
        val inputVar =
          func.vparamss.flatten
            .collectFirst {
              case field @ ValDef(Modifiers(_, _, annotations), _, _, _)
                  if annotations.exists(_ equalsStructure q"new FlinkMsg()") =>
                field
            }

        val higherKindedType =
          func.vparamss.flatten
            .map(_.tpt)
            .collectFirst {
              case tq"TypedStatefulFunction[$higherKindedType, $_, $returnType]" =>
                (higherKindedType.asInstanceOf[Ident], returnType.some)
              case tq"$_#$_[$higherKindedType]" =>
                (higherKindedType.asInstanceOf[Ident], none)
              case tq"StatefulFunction[$higherKindedType, $_]" =>
                (higherKindedType.asInstanceOf[Ident], none)
            }

        val inputMappings =
          func.vparamss.flatten
            .collectFirst {
              case field @ ValDef(Modifiers(_, _, annotations), _, _, _)
                  if annotations.exists(_ equalsStructure q"new FlinkMsg()") =>
                field
            }

        val mappings = func.mods.annotations.collect {
          case q"new MapInputs(..$funcs)" => funcs.map(_.asInstanceOf[Function])
        }.flatten

        (inputVar product higherKindedType)
          .map {
            case (input, (higherKindedType, typedReturn)) =>
              val monad = tq"${symbolOf[Monad.type].companion}"
              val address = tq"${symbolOf[Address.type].companion}"
              val flinkError = tq"${symbolOf[FlinkError.type].companion}"
              val handle = tq"${symbolOf[Handle.type].companion}"
              val statefulFunction = tq"${symbolOf[StatefulFunction.type].companion}"
              val typedStateFun = tq"${symbolOf[TypedStatefulFunction.type].companion}"
              val canAccept = tq"${symbolOf[CanAccept.type].companion}"
              val canAcceptCons =
                tq"${symbolOf[CanAccept.type].asClass.module.info.member(TermName("apply"))}"
              val functionDescriptor = tq"${symbolOf[FunctionDescriptor]}"
              val codec = tq"${symbolOf[Codec.type].asClass.module.info.member(TermName("apply"))}"
              val typeIdents = func.tparams.map(_.name).map(Ident(_))
              val nonInputParams = func.vparamss.map(_.filter(_ != input))
              val nonFlinkInputs = nonInputParams.init :+ nonInputParams.last ++
                List(
                  ValDef(
                    Modifiers(Flag.IMPLICIT | Flag.PARAM),
                    TermName(c.freshName()),
                    AppliedTypeTree(
                      handle,
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
              val AppliedTypeTree(Ident(TypeName(_)), List(returnType)) = func.tpt

              def serializerGenerator(wrapper: Symbol): DefDef =
                q"""
                 def serializedInput[..${func.tparams}](...$nonFlinkInputs) = {
                   $wrapper[$higherKindedType, ${msgValDef.tpt}].andThen(($msgValDef) => apply[..${typeIdents}](...$withFlinkInputs)).run
                 }
                 """

              def mappingSerializer(mapper: Symbol, mappers: List[Tree]): DefDef =
                q"""
                 def serializedInput[..${func.tparams}](...$nonFlinkInputs) = {
                   $mapper[$higherKindedType, ${msgValDef.tpt}, $returnType](..$mappers)(($msgValDef) => apply[..${typeIdents}](...$withFlinkInputs))
                 }
                 """

              val serializedInputDef = {
                val codecMapping =
                  symbolOf[StatefulFunction.type].asClass.module.info
                    .member(TermName("codecMapping"))
                val codecInputK =
                  symbolOf[StatefulFunction.type].asClass.module.info
                    .member(TermName("codecInputK"))
                val mappingSerialized = mappings.map(mapping =>
                  q"$codecInputK[$higherKindedType, ${mapping.vparams.head.tpt}].map(${mapping}).run"
                )
                if (mappings.isEmpty) serializerGenerator(codecInputK)
                else mappingSerializer(codecMapping, mappingSerialized)
              }

              val fName = TypeName(c.freshName())
              val stateName = TypeName(c.freshName())
              val objName = TermName(func.name.toString)
              val msgPassing = typedReturn.fold(
                q"""
                def send[$fName[_], $stateName](id: String, msg: ${input.tpt})(implicit statefun: $statefulFunction[$fName, $stateName]) =
                  statefun.sendMsg($namespace, $tpe, id, $codec[${input.tpt}].pack(msg))
                """
              ) { typedReturn =>
                q"""
                def ask[$fName[_]: $monad, $stateName](id: String, func: $address => ${input.tpt})(implicit canAccept: $canAccept[$typedReturn], statefun: $statefulFunction[$fName, $stateName]) =
                  statefun.myAddr.flatMap { replyTo =>
                    statefun.sendMsg($namespace, $tpe, id, $codec[${input.tpt}].pack(func(replyTo)))
                  }
                """
              }
              val canAcceptImplicits = mappings.map(mapping =>
                q"implicit val ${TermName(c.freshName())}: $canAccept[${mapping.vparams.head.tpt}] = new $canAccept[${mapping.vparams.head.tpt}]"
              )
              q"""
              object $objName extends $functionDescriptor {
                override val namespaceType = ($namespace, $tpe)
                ..${canAcceptImplicits}
                def apply[..${func.tparams}](...${func.vparamss}): ${func.tpt} = ${func.rhs}
                ..${serializedInputDef}
                ..${msgPassing}
              }
              """
          }
          .getOrElse(EmptyTree)
      case _ => EmptyTree
    }
    out
  }
}
