/*
 * Copyright 2015 Databricks
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.scalastyle.sbt.ScalastylePlugin.rawScalastyleSettings
import sbt.Keys._
import sbt._
import sbtrelease.ReleasePlugin.autoImport.ReleaseTransformations._
import sbtrelease.ReleasePlugin.autoImport._
import scoverage.ScoverageKeys

val sparkVersion = "3-3-2-aiq99"

// Define a custom test configuration so that unit test helper classes can be re-used under
// the integration tests configuration; see http://stackoverflow.com/a/20635808.
lazy val IntegrationTest = config("it") extend Test
val testSparkVersion = sys.props.get("spark.testVersion").getOrElse(sparkVersion)
val testHadoopVersion = sys.props.get("hadoop.testVersion").getOrElse("3.3.2")
// DON't UPGRADE AWS-SDK-JAVA if not compatible with hadoop version
val testAWSJavaSDKVersion = sys.props.get("aws.testVersion").getOrElse("1.12.31")

resolvers += "Artifactory".at("https://actioniq.jfrog.io/artifactory/aiq-sbt-local/")


lazy val root = Project("spark-redshift", file("."))
  .configs(IntegrationTest)
  .settings(Project.inConfig(IntegrationTest)(rawScalastyleSettings()): _*)
  .settings(Defaults.coreDefaultSettings: _*)
  .settings(Defaults.itSettings: _*)
  .enablePlugins(PublishToArtifactory)
  .settings(
    name := "spark-redshift",
    organization := "io.github.spark-redshift-community",
    scalaVersion := "2.12.15",
    licenses += "Apache-2.0" -> url("http://opensource.org/licenses/Apache-2.0"),
    scalacOptions ++= Seq("-release", "17"),
    javacOptions ++= Seq("-source", "17", "-target", "17"),
    libraryDependencies ++= Seq(
      "org.slf4j" % "slf4j-api" % "1.7.32",
      "com.eclipsesource.minimal-json" % "minimal-json" % "0.9.4",

      // A Redshift-compatible JDBC driver must be present on the classpath for spark-redshift to work.
      // For testing, we use an Amazon driver, which is available from
      // http://docs.aws.amazon.com/redshift/latest/mgmt/configure-jdbc-connection.html
      "com.amazon.redshift" % "jdbc42" % "2.1.0.14" % "test" from "https://s3.amazonaws.com/redshift-downloads/drivers/jdbc/2.1.0.14/redshift-jdbc42-2.1.0.14.jar",

      "com.google.guava" % "guava" % "20.0",
      "org.scalatest" %% "scalatest" % "3.0.5" % "test",
      "org.mockito" % "mockito-core" % "1.10.19" % "test",

      "com.amazonaws" % "aws-java-sdk" % testAWSJavaSDKVersion % "provided" excludeAll
        (ExclusionRule(organization = "com.fasterxml.jackson.core")),

      "org.apache.hadoop" % "hadoop-client" % testHadoopVersion % "provided" exclude("javax.servlet", "servlet-api") force(),
      "org.apache.hadoop" % "hadoop-common" % testHadoopVersion % "provided" exclude("javax.servlet", "servlet-api") force(),
      "org.apache.hadoop" % "hadoop-common" % testHadoopVersion % "provided" classifier "tests" force(),

      "org.apache.hadoop" % "hadoop-aws" % testHadoopVersion excludeAll
        (ExclusionRule(organization = "com.fasterxml.jackson.core"))
        exclude("org.apache.hadoop", "hadoop-common")
        exclude("com.amazonaws", "aws-java-sdk-bundle")  force(), // load from provided aws-java-sdk-* instead of bundle

      "org.apache.spark" %% "spark-core" % testSparkVersion % "provided" exclude("org.apache.hadoop", "hadoop-client") force(),
      "org.apache.spark" %% "spark-sql" % testSparkVersion % "provided" exclude("org.apache.hadoop", "hadoop-client") force(),
      "org.apache.spark" %% "spark-hive" % testSparkVersion % "provided" exclude("org.apache.hadoop", "hadoop-client") force(),
      "org.apache.spark" %% "spark-avro" % testSparkVersion % "provided" exclude("org.apache.avro", "avro-mapred") force()
    ),
    ScoverageKeys.coverageHighlighting := true,
    logBuffered := false,
    // Display full-length stacktraces from ScalaTest:
    testOptions in Test += Tests.Argument("-oF"),
    fork in Test := true,
    javaOptions in Test ++= Seq(
      "-Xms512M", "-Xmx2048M",
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens=java.base/java.lang=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
      "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
      "--add-opens=java.base/java.util=ALL-UNNAMED",
    ),

    publishMavenStyle := true,
    releaseCrossBuild := true,
    licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0")),

    pomExtra :=
    <url>https://github.com:spark_redshift_community/spark.redshift</url>
    <scm>
      <url>git@github.com:spark_redshift_community/spark.redshift.git</url>
      <connection>scm:git:git@github.com:spark_redshift_community/spark.redshift.git</connection>
    </scm>
    <developers>
      <developer>
        <id>meng</id>
        <name>Xiangrui Meng</name>
        <url>https://github.com/mengxr</url>
      </developer>
      <developer>
        <id>JoshRosen</id>
        <name>Josh Rosen</name>
        <url>https://github.com/JoshRosen</url>
      </developer>
      <developer>
        <id>marmbrus</id>
        <name>Michael Armbrust</name>
        <url>https://github.com/marmbrus</url>
      </developer>
      <developer>
        <id>lucagiovagnoli</id>
        <name>Luca Giovagnoli</name>
        <url>https://github.com/lucagiovagnoli</url>
      </developer>
    </developers>,

    // Add publishing to spark packages as another step.
    releaseProcess := Seq[ReleaseStep](
      checkSnapshotDependencies,
      inquireVersions,
      runTest,
      setReleaseVersion,
      commitReleaseVersion,
      tagRelease,
      publishArtifacts,
      setNextVersion,
      commitNextVersion,
      pushChanges
    )
  )
