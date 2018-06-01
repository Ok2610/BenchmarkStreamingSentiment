name := "FlinkCoreNlp"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
	// https://mvnrepository.com/artifact/org.apache.flink/flink-streaming-scala
	"org.apache.flink" %% "flink-streaming-scala" % "1.4.0",
	// https://mvnrepository.com/artifact/org.apache.flink/flink-connector-kafka-0.10
	"org.apache.flink" %% "flink-connector-kafka-0.10" % "1.4.0",
	// https://mvnrepository.com/artifact/org.apache.flink/flink-connector-kafka-base
	"org.apache.flink" %% "flink-connector-kafka-base" % "1.4.0",
	// https://mvnrepository.com/artifact/org.apache.kafka/kafka
	"org.apache.kafka" %% "kafka" % "0.10.2.1",
	// https://mvnrepository.com/artifact/edu.stanford.nlp/stanford-corenlp
	"edu.stanford.nlp" % "stanford-corenlp" % "3.9.1" artifacts (Artifact("stanford-corenlp", "models-english"), Artifact("stanford-corenlp")),
	"com.typesafe" % "config" % "1.3.2"
	)

assemblyMergeStrategy in assembly := {
  case m if m.toLowerCase.endsWith("manifest.mf")          => MergeStrategy.discard
  case m if m.toLowerCase.matches("meta-inf.*\\.sf$")      => MergeStrategy.discard
  case "log4j.properties"                                  => MergeStrategy.discard
  case m if m.toLowerCase.startsWith("meta-inf/services/") => MergeStrategy.filterDistinctLines
  case "reference.conf"                                    => MergeStrategy.concat
  case _                                                   => MergeStrategy.first
}

mainClass in Compile := Some("FlinkCoreNlp.Main")
