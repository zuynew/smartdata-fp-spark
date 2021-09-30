logLevel := Level.Warn

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.15.0")

addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.3.0")

//addSbtPlugin("ch.epfl.scala" % "sbt-scalafix" % "0.9.25")

addSbtPlugin("com.timushev.sbt" % "sbt-rewarn" % "0.1.1")

addSbtPlugin("io.github.davidgregory084" % "sbt-tpolecat" % "0.1.13")

addSbtPlugin("com.timushev.sbt" % "sbt-updates" % "0.5.1")

addSbtPlugin("com.thesamet"                    % "sbt-protoc"     % "1.0.0-RC4")
libraryDependencies += "com.thesamet.scalapb" %% "compilerplugin" % "0.11.0-M4"
