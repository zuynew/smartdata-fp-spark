package ru.ozon

import org.apache.spark.sql.{SaveMode, SparkSession}
import zio.ZManaged

import java.time.temporal.ChronoUnit

class SparkJob(spark: SparkSession, verticaCfg: runtime.Vertica) {
  def run(partition: String, callsTable: String, chatsTable: String): zio.IO[errors.ApplicationError, Unit] = {
    import spark.implicits._

    val read             = new ru.ozon.spark.read(spark)
    val write            = new ru.ozon.spark.write
    val func             = new ru.ozon.spark.functions
    def verticaWriter[T] = write.vertica[T](
      host = verticaCfg.host,
      user = verticaCfg.user,
      db = verticaCfg.db,
      password = verticaCfg.password,
      stagingFsUrl = verticaCfg.stagingFsUrl,
    ) _

    (ZManaged.fromEffect(
      read.table[data.Call]("dim_client")
    ) >>= func.broadcast)
      .use(dimClientDs => {
        (
          for {
            factCallRawDs <- read.csv[data.Call](s"fact_call/date=${partition}/")
            factCallDs     = factCallRawDs.map(call =>
                               call.copy(
                                 startAt = call.startAt.minus(3, ChronoUnit.HOURS),
                                 endAt = call.endAt.minus(3, ChronoUnit.HOURS),
                               )
                             )
            // Сломается тк два разных формата даты
            factChatDs    <- read.json[data.ChatMessage](s"fact_chat/date=${partition}/")

            calls = dimClientDs
                      .join(factCallDs, $"clients.id" === $"calls.clientId")
                      .as[data.ClientCall]
            chats = dimClientDs
                      .join(factChatDs, $"clients.id" === $"chats.clientId")
                      .as[data.ChatMessage]
            _    <- verticaWriter(callsTable, SaveMode.Overwrite)(calls)
            _    <- verticaWriter(chatsTable, SaveMode.Overwrite)(chats)
          } yield ()
        )
      })
      .mapError(errors.ApplicationError.ExecutionError)

  }
}
