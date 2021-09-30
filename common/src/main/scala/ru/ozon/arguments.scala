package ru.ozon

import com.monovore.decline.{Command, Opts}
import cats.syntax.apply._

object arguments {
  val partitionOpt  = Opts.option[String]("partition", short = "p", metavar = "2021-09-21", help = "Partition date")
  val chatsTableOpt = Opts.option[String]("chatsTable", metavar = "chats_stg_table", help = "Chats target table")
  val callsTableOpt = Opts.option[String]("callsTable", metavar = "calls_stg_table", help = "Calls  target table")

  val command = Command(
    name = "job",
    header = "Call center job",
    helpFlag = true
  )(
    (partitionOpt, chatsTableOpt, callsTableOpt).tupled
  )
}
