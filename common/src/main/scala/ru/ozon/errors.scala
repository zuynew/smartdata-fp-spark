package ru.ozon

import cats.data.NonEmptyList
import com.monovore.decline.Help
import pureconfig.error.ConfigReaderFailures

object errors {

  sealed trait ApplicationError extends Throwable

  object ApplicationError {

    case class CommandlineArgumentsError(help: Help)                                   extends ApplicationError
    case class ConfigError(failures: ConfigReaderFailures)                             extends ApplicationError
    case class SparkSessionError(t: Throwable)                                         extends ApplicationError
    case class VaultInitializationError(t: Throwable)                                  extends ApplicationError
    case class VerticaConfigurationInitializationError(failures: NonEmptyList[String]) extends ApplicationError
    case class ExecutionError(t: Throwable)                                            extends ApplicationError

  }

}
