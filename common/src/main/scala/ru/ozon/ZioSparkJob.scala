package ru.ozon

import cats.syntax.either._
import cats.syntax.apply._
import cats.syntax.foldable._
import cats.data.Validated
import com.bettercloud.vault.{Vault, VaultConfig}
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{SaveMode, SparkSession}
import pureconfig.ConfigSource
import ru.ozon.errors.ApplicationError
import zio.Cause.{Die, Fail}
import zio.{Exit, Task, UIO, ZManaged}

import java.time.temporal.ChronoUnit
import scala.jdk.CollectionConverters._
object ZioSparkJob extends App {
  val cfg = ConfigFactory.load()

  val partition: String   = args(0)
  val tableSuffix: String = args(1)

  val spark = SparkSession.builder
    .appName("Application")
    .getOrCreate()

  (for {
    cfg       <- ZManaged.fromEither(
                   ConfigSource
                     .fromConfig(ConfigFactory.load())
                     .load[config.AppConfig]
                     .leftMap(errors.ApplicationError.ConfigError)
                 )
    arguments <-
      ZManaged.fromEither(
        arguments.command
          .parse(args, sys.env)
          .leftMap(errors.ApplicationError.CommandlineArgumentsError)
      )
    spark     <- ZManaged
                   .make(
                     Task(
                       SparkSession.builder
                         .appName("Application")
                         .getOrCreate()
                     )
                   )(ss => UIO(ss.stop()))
                   .mapError(errors.ApplicationError.ExecutionError)
  } yield runtime.ContextV1(cfg, runtime.Args.tupled(arguments), spark))
    .use(ctx => { ??? })

  val eff = (for {
    cfg        <- ZManaged.fromEither(
                    ConfigSource
                      .fromConfig(ConfigFactory.load())
                      .load[config.AppConfig]
                      .leftMap(errors.ApplicationError.ConfigError)
                  )
    arguments  <-
      ZManaged
        .fromEither(
          arguments.command.parse(args, sys.env).leftMap(errors.ApplicationError.CommandlineArgumentsError)
        )
        .map(runtime.Args.tupled)
    vaultConfig = new VaultConfig()
                    .address(cfg.vault.address)
                    .token(cfg.vault.token)
                    .build()
    vault      <-
      ZManaged.fromEffect(Task(new Vault(vaultConfig))).mapError(errors.ApplicationError.VaultInitializationError)
    cfgMap     <- ZManaged.fromEffect(Task(vault.logical().read("etl/dbs/vertica").getData.asScala))
    verticaCfg <-
      ZManaged
        .fromEither(
          (
            Validated.fromOption(cfgMap.get("host"), "host is not set").toValidatedNel,
            Validated.fromOption(cfgMap.get("user"), "user is not set").toValidatedNel,
            Validated.fromOption(cfgMap.get("db"), "db is not set").toValidatedNel,
            Validated.fromOption(cfgMap.get("password"), "password is not set").toValidatedNel,
            Validated.fromOption(cfgMap.get("stagingFsUrl"), "stagingFsUrl is not set").toValidatedNel,
          ).tupled.toEither.leftMap(errors.ApplicationError.VerticaConfigurationInitializationError)
        )
        .map(runtime.Vertica.tupled)

    spark <- ZManaged
               .make(Task(SparkSession.builder.appName("Simple Application").getOrCreate()))(ss => UIO(ss.stop()))
               .mapError(errors.ApplicationError.ExecutionError)

  } yield runtime.ContextV3(cfg, arguments, spark, verticaCfg))
    .use(ctx => {
      import ctx.spark.implicits._

      val read             = new ru.ozon.spark.read(ctx.spark)
      val write            = new ru.ozon.spark.write
      val func             = new ru.ozon.spark.functions
      def verticaWriter[T] = write.vertica[T](
        host = ctx.vertica.host,
        user = ctx.vertica.user,
        db = ctx.vertica.db,
        password = ctx.vertica.password,
        stagingFsUrl = ctx.vertica.stagingFsUrl,
      ) _

      (ZManaged.fromEffect(
        read.table[data.Call]("dim_client")
      ) >>= func.broadcast).use(dimClientDs => {
        (
          for {
            factCallRawDs <- read.csv[data.Call](s"fact_call/date=${ctx.args.partition}/")
            factCallDs     = factCallRawDs.map(call =>
                               call.copy(
                                 startAt = call.startAt.minus(3, ChronoUnit.HOURS),
                                 endAt = call.endAt.minus(3, ChronoUnit.HOURS),
                               )
                             )
            factChatDs    <- read.json[data.ChatMessage](s"fact_chat/date=${ctx.args.partition}/")

            calls = dimClientDs
                      .join(factCallDs, $"clients.id" === $"calls.clientId")
                      .as[data.ClientCall]
            chats = dimClientDs
                      .join(factChatDs, $"clients.id" === $"chats.clientId")
                      .as[data.ChatMessage]
            _    <- verticaWriter(ctx.args.callsTable, SaveMode.Overwrite)(calls)
            _    <- verticaWriter(ctx.args.chatsTable, SaveMode.Overwrite)(chats)
          } yield ()
        ).mapError(errors.ApplicationError.ExecutionError)
      })

    })

  zio.Runtime.default.unsafeRunSync(eff) match {
    case Exit.Success(_) => sys.exit(0)
    case Exit.Failure(cause) =>
      cause match {
        case Fail(value) =>
          value match {
            case ApplicationError.CommandlineArgumentsError(help)                   => println(help)
            case ApplicationError.ConfigError(failures)                             => failures.prettyPrint()
            case ApplicationError.SparkSessionError(t)                              => t.printStackTrace()
            case ApplicationError.VaultInitializationError(t)                       => t.printStackTrace()
            case ApplicationError.VerticaConfigurationInitializationError(failures) =>
              println(failures.mkString_("\n"))
            case ApplicationError.ExecutionError(t)                                 => t.printStackTrace()
          }
        case Die(value)  => value.printStackTrace()
        case _           => ()
      }
      sys.exit(1)
  }
}
