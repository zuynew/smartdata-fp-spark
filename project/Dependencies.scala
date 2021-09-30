import sbt._
import Keys._

object Dependencies {

  object Version {

    val decline = "2.1.0"

    val vault = "5.1.0"

    val spark = "3.1.2"

    val cats = "2.3.0"

    val catsEffect = "2.3.0"

    val derevo = "0.11.5"

    val slf4j = "1.7.30"

    val logback = "1.2.3"

    val typesafeConfig = "1.4.1"

    val zio = "1.0.3"

    val zioCats = "2.2.0.1"

    val distage = "1.0.5"

    val pureConfig = "0.14.0"

    // Compile time only
    val macroParadise = "2.1.1"

    val simulacrum = "1.0.1"

    val kindProjector = "0.13.2"

    val betterMonadicFor = "0.3.1"

    val collectionCompat = "2.3.1"

  }

  val catsCore         = "org.typelevel"         %% "cats-core"         % Version.cats
  val catsEffect       = "org.typelevel"         %% "cats-effect"       % Version.catsEffect
  val decline          = "com.monovore"          %% "decline"           % Version.decline
  val pureConfig       = "com.github.pureconfig" %% "pureconfig"        % Version.pureConfig
  val derevo           = "org.manatki"           %% "derevo-core"       % Version.derevo
  val derevoPureconfig = "org.manatki"           %% "derevo-pureconfig" % Version.derevo
  val zio              = "dev.zio"               %% "zio"               % Version.zio
  val distageCore      = "io.7mind.izumi"        %% "distage-core"      % Version.distage

  val logback          = "ch.qos.logback"          % "logback-classic"         % Version.logback
  val slf4j            = "org.slf4j"               % "slf4j-api"               % Version.slf4j % Provided
  val zioCats          = "dev.zio"                %% "zio-interop-cats"        % Version.zioCats
  val collectionCompat = "org.scala-lang.modules" %% "scala-collection-compat" % Version.collectionCompat

  val typesafeConfig = "com.typesafe" % "config" % Version.typesafeConfig

  val distageConfig = "io.7mind.izumi" %% "distage-extension-config" % Version.distage

  val spark = "org.apache.spark" %% "spark-sql" % Version.spark % Provided

  val vault = "com.bettercloud" % "vault-java-driver" % Version.vault

  // Compile-time only
  val macroParadise    = "org.scalamacros" % "paradise"           % Version.macroParadise cross CrossVersion.patch
  val kindProjector    = "org.typelevel"  %% "kind-projector"     % Version.kindProjector cross CrossVersion.patch
  val simulacrum       = "org.typelevel"  %% "simulacrum"         % Version.simulacrum
  val betterMonadicFor = "com.olegpy"     %% "better-monadic-for" % Version.betterMonadicFor
}
