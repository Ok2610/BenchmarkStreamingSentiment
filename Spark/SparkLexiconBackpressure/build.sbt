name := "SparkLexiconBackpressure"

// Project version number
version := "1.0"

// Scala version used
scalaVersion := "2.11.1"

scalacOptions ++= Seq("-unchecked", "-deprecation")

// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming-kafka-0-10
libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-sql" % "2.2.1",
    "com.github.benfradet" %% "spark-kafka-writer" % "0.4.0",
    "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.2.1",
    "org.apache.kafka" %% "kafka" % "0.10.2.1",
    "org.apache.spark" %% "spark-core" % "2.2.1",
    "org.apache.spark" %% "spark-streaming" % "2.2.1" % "provided",
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


//assemblyMergeStrategy in assembly := {
//  case PathList("org","aopalliance", xs @ _*) => MergeStrategy.last
//  case PathList("javax", "inject", xs @ _*) => MergeStrategy.last
//  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.last
//  case PathList("javax", "activation", xs @ _*) => MergeStrategy.last
//  case PathList("org", "apache", xs @ _*) => MergeStrategy.last
//  case PathList("com", "google", xs @ _*) => MergeStrategy.last
//  case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.last
//  case PathList("com", "codahale", xs @ _*) => MergeStrategy.last
//  case PathList("com", "yammer", xs @ _*) => MergeStrategy.last
//  case "about.html" => MergeStrategy.rename
//  case "META-INF/ECLIPSEF.RSA" => MergeStrategy.last
//  case "META-INF/mailcap" => MergeStrategy.last
//  case "META-INF/mimetypes.default" => MergeStrategy.last
//  case "plugin.properties" => MergeStrategy.last
//  case "log4j.properties" => MergeStrategy.last
//  case x =>
//    val oldStrategy = (assemblyMergeStrategy in assembly).value
//    oldStrategy(x)
//}
