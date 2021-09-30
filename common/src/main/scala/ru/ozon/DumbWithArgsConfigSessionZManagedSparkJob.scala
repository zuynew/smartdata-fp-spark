package ru.ozon

import cats.syntax.either._
import com.bettercloud.vault.{Vault, VaultConfig}
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}
import pureconfig.ConfigSource
import ru.ozon.errors.ApplicationError
import zio.Cause.{Die, Fail}
import zio.{Exit, Task, UIO, ZManaged}

import scala.jdk.CollectionConverters._

object DumbWithArgsConfigSessionZManagedSparkJob extends App {

  val eff = (for {
    cfg       <- ZManaged.fromEither(
                   ConfigSource
                     .fromConfig(ConfigFactory.load())
                     .load[config.AppConfig]
                     .leftMap(errors.ApplicationError.ConfigError)
                 )
    arguments <-
      ZManaged.fromEither(
        arguments.command.parse(args, sys.env).leftMap(errors.ApplicationError.CommandlineArgumentsError)
      )
    spark     <- ZManaged
                   .make(Task(SparkSession.builder.appName("Simple Application").getOrCreate()))(ss => UIO(ss.stop()))
                   .mapError(errors.ApplicationError.ExecutionError)
  } yield runtime.ContextV1(cfg, runtime.Args.tupled(arguments), spark))
    .use(ctx => {
      Task {
        val vaultConfig: VaultConfig = new VaultConfig()
          .address(ctx.cfg.vault.address)
          .token(ctx.cfg.vault.token)
          .build()

        val vault: Vault = new Vault(vaultConfig)

        val verticaOpts = vault.logical().read("etl/dbs/vertica").getData.asScala

        import ctx.spark.implicits._

        val dimClientDs = broadcast(ctx.spark.read.table("dim_client").as[data.Client].alias("clients").cache())

        val factCallDs = ctx.spark.read
          .csv(s"fact_call/date=${ctx.args.partition}/")
          .withColumn(
            "startAt",
            to_utc_timestamp($"startAt", "MSK")
          )
          .withColumn(
            "endAt",
            to_utc_timestamp($"endAt", "MSK")
          )
          .as[data.Call]
          .alias("calls")

        val factChatDs = ctx.spark.read
          .json(s"fact_chat/date=${ctx.args.partition}/")
          .withColumn("messages", $"messages".withField("sendAt", to_timestamp($"messages.sendAt", "MM/dd/yyyy")))
          .as[data.ChatMessage]
          .alias("chats")

        val calls = dimClientDs.join(factCallDs, $"clients.id" === $"calls.clientId")
        val chats = dimClientDs.join(factChatDs, $"clients.id" === $"chats.clientId")

        calls.write
          .format("com.vertica.spark.datasource.DefaultSource")
          .options(verticaOpts + ("table" -> ctx.args.callsTable))
          .mode(SaveMode.Overwrite)
          .save()

        chats.write
          .format("com.vertica.spark.datasource.DefaultSource")
          .options(verticaOpts + ("table" -> ctx.args.chatsTable))
          .mode(SaveMode.Overwrite)
          .save()

        dimClientDs.unpersist()
      }.mapError(errors.ApplicationError.ExecutionError)
    })

  zio.Runtime.default.unsafeRunSync(eff) match {
    case Exit.Success(_) => sys.exit(0)
    case Exit.Failure(cause) =>
      cause match {
        case Fail(value) =>
          value match {
            case ApplicationError.CommandlineArgumentsError(help) => println(help)
            case ApplicationError.ConfigError(failures)           => failures.prettyPrint()
            case ApplicationError.SparkSessionError(t)            => t.printStackTrace()
            case ApplicationError.ExecutionError(t)               => t.printStackTrace()
          }
        case Die(value)  => value.printStackTrace()
        case _           => ()
      }
      sys.exit(1)
  }
}
