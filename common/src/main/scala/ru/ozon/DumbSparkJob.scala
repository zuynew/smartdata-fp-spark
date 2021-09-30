package ru.ozon

import com.bettercloud.vault.{Vault, VaultConfig}
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable
import scala.jdk.CollectionConverters._

object DumbSparkJob extends App {

  val cfg = ConfigFactory.load()

  val partition: String   = args(0)
  val tableSuffix: String = args(1)

  val vaultConfig: VaultConfig = new VaultConfig()
    .address(cfg.getString("vault.address"))
    .token(cfg.getString("vault.token"))
    .build()

  val vault: Vault = new Vault(vaultConfig)

  val verticaOpts = vault.logical().read("etl/dbs/vertica").getData.asScala

  val spark = SparkSession.builder.appName("Simple Application").getOrCreate()

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
    .options(verticaOpts + ("table" -> s"calls$tableSuffix"))
    .mode(SaveMode.Overwrite)
    .save()

  chats.write
    .format("com.vertica.spark.datasource.DefaultSource")
    .options(verticaOpts + ("table" -> s"chats$tableSuffix"))
    .mode(SaveMode.Overwrite)
    .save()

  dimClientDs.unpersist()

  spark.stop()

}
