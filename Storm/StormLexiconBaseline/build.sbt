name := "Storm"

version := "1.0"

scalaVersion := "2.12.3"

mainClass in Compile := Some("Sentiment.CoreNlpTopology")

libraryDependencies ++= Seq(
  "org.apache.storm" % "storm-core" % "1.0.2" % "provided" exclude("junit", "junit"),
  "org.apache.storm" % "storm-kafka" % "1.0.0",
  //"org.apache.storm" % "storm-hdfs" % "1.2.0" exclude("org.slf4j", "slf4j-log4j12") exclude("io.confluent", "kafka-avro-serializer"),
  "org.apache.hadoop" % "hadoop-hdfs" % "2.2.0" exclude("org.slf4j", "slf4j-log4j12") exclude("xml-apis", "xml-apis"),
  "org.apache.hadoop" % "hadoop-client" % "2.2.0" exclude("org.slf4j", "slf4j-log4j12"),

  //"org.apache.maven.plugins" % "maven-shade-plugin" % "3.1.0",
  // "javax.persistence" % "persistence-api" % "1.0",

  "org.scala-lang" % "scala-library" % scalaVersion.value,
  "commons-collections" % "commons-collections" % "3.2.1",
  "org.apache.kafka" %% "kafka" % "1.0.0" exclude("log4j", "log4j") exclude("org.apache.zookeeper", "zookeeper") exclude("org.slf4j", "slf4j-log4j12"),
  "com.typesafe" % "config" % "1.3.2",
  //"edu.stanford.nlp" % "stanford-corenlp" % "3.9.1" artifacts (Artifact("stanford-corenlp", "models-english"), Artifact("stanford-corenlp"))
)

scalacOptions += "-Yresolve-term-conflict:package"
fork := true
resolvers ++= Seq(
  "apache-snapshots" at "http://repository.apache.org/snapshots/"
)

//assemblyShadeRules in assembly := Seq(
//  ShadeRule.rename("org.apache.commons.io.**" -> "shadeio.@1").inAll,
//  ShadeRule.zap("a.b.c").inAll,
//  ShadeRule.keep("x.**").inAll
//)

assemblyMergeStrategy in assembly := {
  case m if m.toLowerCase.endsWith("manifest.mf")          => MergeStrategy.discard
  case m if m.toLowerCase.matches("meta-inf.*\\.sf$")      => MergeStrategy.discard
  case "log4j.properties"                                  => MergeStrategy.discard
  case m if m.toLowerCase.startsWith("meta-inf/services/") => MergeStrategy.filterDistinctLines
  case "reference.conf"                                    => MergeStrategy.concat
  case _                                                   => MergeStrategy.first
}