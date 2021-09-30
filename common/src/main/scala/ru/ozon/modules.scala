package ru.ozon
import cats.syntax.apply._
import cats.syntax.either._
import cats.data.Validated
import com.bettercloud.vault.{Vault, VaultConfig}
import distage.{Lifecycle, ModuleDef}
import org.apache.spark.sql.SparkSession
import zio.{Task, UIO, ZManaged}
import distage.config.ConfigModuleDef

import scala.jdk.CollectionConverters._

object modules extends ModuleDef {

  include(appConfig)
  include(spark)
  include(runtimeConfig)
  include(job)

  def spark = new ModuleDef {
    make[SparkSession].fromResource(
      Lifecycle.fromZIO(
        ZManaged
          .make(Task(SparkSession.builder.appName("Simple Application").getOrCreate()))(ss => UIO(ss.stop()))
          .mapError(errors.ApplicationError.ExecutionError)
      )
    )
  }

  def appConfig = new ConfigModuleDef {
    makeConfig[config.AppConfig]("app")
  }

  def vault = new ModuleDef {
    make[Vault].fromEffect((cfg: config.AppConfig) =>
      Task(
        new Vault(
          new VaultConfig()
            .address(cfg.vault.address)
            .token(cfg.vault.token)
            .build()
        )
      ).mapError(errors.ApplicationError.VaultInitializationError)
    )
  }

  def runtimeConfig = new ModuleDef {
    make[runtime.Vertica].fromEffect((v: Vault) =>
      Task(v.logical().read("etl/dbs/vertica").getData.asScala).flatMap(cfgMap =>
        Task
          .fromEither(
            (
              Validated.fromOption(cfgMap.get("host"), "host is not set"),
              Validated.fromOption(cfgMap.get("user"), "user is not set"),
              Validated.fromOption(cfgMap.get("db"), "db is not set"),
              Validated.fromOption(cfgMap.get("password"), "password is not set"),
              Validated.fromOption(cfgMap.get("stagingFsUrl"), "stagingFsUrl is not set"),
            ).tupled.toValidatedNel.toEither.leftMap(errors.ApplicationError.VerticaConfigurationInitializationError)
          )
          .map(runtime.Vertica.tupled)
      )
    )
  }

  def job = new ModuleDef {
    make[SparkJob]
  }

}
