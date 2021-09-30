package ru.ozon

import org.apache.spark.sql.{Dataset, Encoder, SaveMode, SparkSession}
import zio.{Task, UIO, ZManaged}
import org.apache.spark.sql.functions.{broadcast => sbroadcast}

object spark {

  class read(spark: SparkSession) {
    def table[T: Encoder](table: String): Task[Dataset[T]] = Task(spark.read.table(table).as[T])
    def csv[T: Encoder](path: String): Task[Dataset[T]]    = Task(spark.read.csv(path).as[T])
    def json[T: Encoder](path: String): Task[Dataset[T]]   = Task(spark.read.json(path).as[T])
    def text(path: String): Task[Dataset[String]]          = Task(spark.read.textFile(path))
  }

  class write {
    def vertica[T](
        host: String,
        user: String,
        db: String,
        password: String,
        stagingFsUrl: String,
    )(table: String, mode: SaveMode)(ds: Dataset[T]): Task[Unit] = Task(
      ds.write
        .format("com.vertica.spark.datasource.DefaultSource")
        .options(
          Map(
            "host"           -> host,
            "user"           -> user,
            "db"             -> db,
            "password"       -> password,
            "staging_fs_url" -> stagingFsUrl,
            "table"          -> table,
          )
        )
        .mode(mode)
        .save()
    )
  }

  class functions {
    def cache[T](ds: Dataset[T]): ZManaged[Any, Throwable, Dataset[T]]     =
      ZManaged.make(Task(ds.cache()))(ds0 => UIO(ds0.unpersist()))
    def broadcast[T](ds: Dataset[T]): ZManaged[Any, Throwable, Dataset[T]] = cache(ds).map(sbroadcast)
  }

}
