name := "dm"
version := "load"
scalaVersion := "2.12.10"

resolvers += "Cloudera Repository" at "https://repository.cloudera.com/content/repositories/releases/"
resolvers += "Cloudera Artifactory" at "https://repository.cloudera.com/artifactory/public/"
resolvers += "Maven Central" at "https://repo1.maven.org/maven2"


libraryDependencies ++= Seq(
  //Spark
  "org.apache.spark" %% "spark-core" % "3.1.1" % "provided",
  "org.apache.spark" %% "spark-sql" % "3.1.1" % "provided",
  "org.apache.spark" %% "spark-hive" % "3.1.1" % "provided",

  //ElasticSearch
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
