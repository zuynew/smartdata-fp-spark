import Dependencies._

val scalaV = "2.12.15"

lazy val defaultSettings = Seq(
  scalaVersion := scalaV,
  defaultScalacOptions,
  assemblyMergeStrategy in assembly := {
    case PathList("META-INF", "services", xs @ _*) => MergeStrategy.concat
    case PathList("META-INF", xs @ _*)             => MergeStrategy.discard
    case x if x.endsWith(".conf")                  => MergeStrategy.concat
    case _                                         => MergeStrategy.first
  },
  libraryDependencies ++= Seq(
    compilerPlugin(kindProjector),
    compilerPlugin(betterMonadicFor),
    compilerPlugin(macroParadise),
    collectionCompat,
    scalaOrganization.value % "scala-reflect" % scalaVersion.value % Provided
  )
) ++ simulacrumOptions

lazy val common = project
  .in(file("common"))
  .settings(
    defaultSettings,
    libraryDependencies ++= Seq(
      catsCore,
      catsEffect,
      zio,
      zioCats,
      distageCore,
      distageConfig,
      spark,
      vault,
      decline,
      derevoPureconfig,
    ),
  )

lazy val defaultScalacOptions = scalacOptions := {
  val tpolecatOptions = scalacOptions.value

  val dropLints = Set(
    "-Ywarn-dead-code",
    "-Wdead-code" // ignore dead code paths where `Nothing` is involved
  )

  val opts = tpolecatOptions.filterNot(dropLints) ++ Set("-language:postfixOps")

  // drop `-Xfatal-warnings` on dev and 2.12 CI
  if (!sys.env.get("CI").contains("true"))
    opts.filterNot(Set("-Xfatal-warnings"))
  else
    opts
}

lazy val simulacrumOptions = Seq(
  libraryDependencies += simulacrum % Provided,
  pomPostProcess := { node =>
    import scala.xml.transform.{RewriteRule, RuleTransformer}

    new RuleTransformer(new RewriteRule {
      override def transform(node: xml.Node): Seq[xml.Node] = node match {
        case e: xml.Elem
            if e.label == "dependency" &&
              e.child.exists(child => child.label == "groupId" && child.text == simulacrum.organization) &&
              e.child.exists(child => child.label == "artifactId" && child.text.startsWith(s"${simulacrum.name}_")) =>
          Nil
        case _ => Seq(node)
      }
    }).transform(node).head
  }
)

addCommandAlias("fmt", "all scalafmtSbt scalafmt test:scalafmt")
addCommandAlias("checkfmt", "all scalafmtSbtCheck scalafmtCheck test:scalafmtCheck")
