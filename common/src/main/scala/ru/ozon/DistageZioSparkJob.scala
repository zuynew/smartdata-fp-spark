package ru.ozon

import cats.syntax.either._
import cats.syntax.foldable._
import distage.Injector
import ru.ozon.errors.ApplicationError
import zio.console.putStr

object DistageZioSparkJob extends zio.App {

  final def run(args: List[String]) =
    myAppLogic(args).exitCode

  def myAppLogic(args: List[String]) =
    (zio.IO.fromEither(
      arguments.command.parse(args, sys.env).leftMap(errors.ApplicationError.CommandlineArgumentsError)
    ) >>= (Injector().produceGet[SparkJob](modules).unsafeGet().run _).tupled).tapError(
      {
        case ApplicationError.CommandlineArgumentsError(help)                   => putStr(help.toString())
        case ApplicationError.ConfigError(failures)                             => putStr(failures.prettyPrint())
        case ApplicationError.SparkSessionError(t)                              => putStr(t.toString)
        case ApplicationError.VaultInitializationError(t)                       => putStr(t.toString)
        case ApplicationError.VerticaConfigurationInitializationError(failures) =>
          putStr(failures.mkString_("\n"))
        case ApplicationError.ExecutionError(t)                                 => putStr(t.toString)
      }
    )

}
