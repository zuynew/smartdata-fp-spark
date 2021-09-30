package ru.ozon

import com.bettercloud.vault.Vault
import org.apache.spark.sql.SparkSession

object runtime {

  case class Args(partition: String, chatsTable: String, callsTable: String)
  case class ContextV1(
      cfg: config.AppConfig,
      args: Args,
      spark: SparkSession
  )
  case class ContextV2(
      cfg: config.AppConfig,
      args: Args,
      spark: SparkSession,
      vault: Vault
  )
  case class ContextV3(
      cfg: config.AppConfig,
      args: Args,
      spark: SparkSession,
      vertica: Vertica
  )

  case class Vertica(
      host: String,
      user: String,
      db: String,
      password: String,
      stagingFsUrl: String,
  )
}
