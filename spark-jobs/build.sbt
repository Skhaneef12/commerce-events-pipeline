ThisBuild / scalaVersion := "2.12.18"
ThisBuild / organization := "commerce"
ThisBuild / version      := "0.1.0"

val sparkVersion = "3.5.1"

lazy val root = (project in file("."))
  .settings(
    name := "commerce-events-pipeline",
    libraryDependencies ++= Seq(
      // Provided: the Spark runtime (spark-submit / Docker image) already has these.
      "org.apache.spark"  %% "spark-sql"             % sparkVersion % Provided,
      "org.apache.spark"  %% "spark-sql-kafka-0-10"  % sparkVersion,
      "io.delta"          %% "delta-spark"           % "3.2.0",
      "org.mongodb.spark" %% "mongo-spark-connector" % "10.3.0",
      ("com.amazon.deequ"  % "deequ"                 % "2.0.7-spark-3.5")
        .excludeAll(ExclusionRule(organization = "org.apache.spark")),
      "org.scalatest"     %% "scalatest"             % "3.2.18" % Test
    ),
    scalacOptions ++= Seq("-deprecation", "-feature", "-unchecked", "-Xlint"),

    // Spark needs these module opens on Java 17.
    Test / fork := true,
    Test / javaOptions ++= Seq(
      "-Duser.timezone=UTC",
      "-Xmx2g",
      "--add-opens=java.base/java.lang=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "--add-opens=java.base/java.util=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED"
    ),
    Test / parallelExecution := false,

    // Fat jar for spark-submit. Service files must be concatenated, not discarded,
    // or the Kafka/Delta/MongoDB data sources won't register.
    assembly / assemblyJarName := "commerce-events-pipeline.jar",
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", "services", _*) => MergeStrategy.concat
      case PathList("META-INF", _*)             => MergeStrategy.discard
      case "reference.conf"                     => MergeStrategy.concat
      case _                                    => MergeStrategy.first
    }
  )
