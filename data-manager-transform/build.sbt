name := "dm"
version := "transform"
scalaVersion := "2.12.10"

resolvers += "Cloudera Repository" at "https://repository.cloudera.com/content/repositories/releases/"
resolvers += "Cloudera Artifcatory" at "https://repository.cloudera.com/artifactory/public/"

libraryDependencies ++= Seq(
  //Spark
  "org.apache.spark" %% "spark-core" % "3.1.1" % "provided",
  "org.apache.spark" %% "spark-sql" % "3.1.1" % "provided",
  "org.apache.spark" %% "spark-hive" % "3.1.1" % "provided",
  //Etc
  "org.apache.httpcomponents" % "httpclient" % "4.5.13",
  "org.apache.httpcomponents" % "httpcore" % "4.4.13",
  "com.typesafe" % "config" % "1.4.2"
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*)                 => MergeStrategy.discard
  case PathList("javax", "servlet", xs @ _*)         => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".html" => MergeStrategy.first
  case "application.conf"                            => MergeStrategy.concat
  case "unwanted.txt"                                => MergeStrategy.discard
  case x                                             => MergeStrategy.first
}

test in assembly := {}

parallelExecution in Test := false
fork in Test := true
javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled")

