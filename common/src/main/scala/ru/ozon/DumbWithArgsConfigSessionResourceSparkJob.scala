package ru.ozon

import cats.effect.Resource
import cats.syntax.either._
import com.bettercloud.vault.{Vault, VaultConfig}
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}
import pureconfig.ConfigSource
import zio.Task
import zio.interop.catz._

import scala.jdk.CollectionConverters._

object DumbWithArgsConfigSessionResourceSparkJob extends App {

  (for {
    cfg                                 <-
      ConfigSource.fromConfig(ConfigFactory.load()).load[config.AppConfig].leftMap(errors.ApplicationError.ConfigError)
    (partition, chatsTable, callsTable) <-
      arguments.command.parse(args, sys.env).leftMap(errors.ApplicationError.CommandlineArgumentsError)
  } yield {

    zio.Runtime.default.unsafeRun(
      Resource
        .make(
          Task(SparkSession.builder.appName("Simple Application").getOrCreate())
        )(ss => Task(ss.stop()))
        .use(spark =>
          Task({
            val vaultConfig: VaultConfig = new VaultConfig()
              .address(cfg.vault.address)
              .token(cfg.vault.token)
              .build()

            val vault: Vault = new Vault(vaultConfig)

            //  val verticaOpts = Map(
            //    "host"           -> "vertica_hostname",
            //    "user"           -> "vertica_user",
            //    "db"             -> "db_name",
            //    "password"       -> "db_password",
            //    "staging_fs_url" -> "hdfs://hdfs-url:7077/data",
            //  )

            val verticaOpts = vault.logical().read("etl/dbs/vertica").getData.asScala

            import spark.implicits._

            val dimClientDs = broadcast(spark.read.table("dim_client").as[data.Client].alias("clients").cache())

            val factCallDs = spark.read
              .csv(s"fact_call/date=$partition/")
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

            val factChatDs = spark.read
              .json(s"fact_chat/date=$partition/")
              .withColumn("messages", $"messages".withField("sendAt", to_timestamp($"messages.sendAt", "MM/dd/yyyy")))
              .as[data.ChatMessage]
              .alias("chats")

            val calls = dimClientDs.join(factCallDs, $"clients.id" === $"calls.clientId")
            val chats = dimClientDs.join(factChatDs, $"clients.id" === $"chats.clientId")

            calls.write
              .format("com.vertica.spark.datasource.DefaultSource")
              .options(verticaOpts + ("table" -> callsTable))
              .mode(SaveMode.Overwrite)
              .save()

            chats.write
              .format("com.vertica.spark.datasource.DefaultSource")
              .options(verticaOpts + ("table" -> chatsTable))
              .mode(SaveMode.Overwrite)
              .save()

            dimClientDs.unpersist()
          })
        )
    )
  }) match {
    case Left(err) =>
      err match {
        case errors.ApplicationError.CommandlineArgumentsError(help) => println(help)
        case errors.ApplicationError.ConfigError(failures)           => println(failures)
      }
      sys.exit(1)
    case _         => sys.exit(0)
  }

}
