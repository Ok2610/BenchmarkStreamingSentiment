package FlinkLexiconBaseline

import java.util.Properties
import java.io.File
import java.time.Instant

import com.typesafe.config.{ Config, ConfigFactory }

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.co.RichCoMapFunction

import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.flink.streaming.util.serialization

import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer010, FlinkKafkaProducer010}

import org.apache.flink.configuration.Configuration

import org.apache.flink.api.common.typeinfo

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import java.util

object Main {

	type Sentiment = (Double, Double)
	type FileLine = (String, Sentiment)

	case class Message(key:String,value:String)

	val config = ConfigFactory.parseFile(new File("/opt/benchmark/application.conf"))

	val wordToSentimentMap = new util.HashMap[String, Sentiment]

	def main(args: Array[String]): Unit = {
		
		fillMap(new Path("hdfs://" + config.getString("cluster.hdfs.host") + config.getString("cluster.hdfs.lexiconPath")))
		//fillMap(new Path("hdfs://h1.itu.dk:8020/user/foa/sentiWord_1_avgScores/"))
		val benchmarkId = args(0).toInt
		val consumerTopic = args(1)
		val producerTopic = args(2)
		val brokers = config.getString("cluster.kafka.brokers")
		val zookeeper = config.getString("cluster.zookeeper.hosts")
		val consumerGroupId = "flink"

		val env = StreamExecutionEnvironment.getExecutionEnvironment

		env.setParallelism(config.getInt("cluster.flink.yarn.slots"))

		val props = Map(
			"zookeeper.connect" -> zookeeper,
			"group.id" -> consumerGroupId,
			"bootstrap.servers" -> brokers
		)

		val kafkaConsumer = new FlinkKafkaConsumer010[String](
			consumerTopic,
			new SimpleStringSchema,
			props
		)

		val kafkaProducer = new FlinkKafkaProducer010(
			brokers,
			producerTopic,
			new SimpleStringSchema
		)

		val kafkaStream = env.addSource(kafkaConsumer)
		
		val wordStream = 
			kafkaStream
			.map(line => {
				val start = Instant.now.toEpochMilli.toString
				val timestamp = line.substring(0, line.indexOf(','))
				val text = line.substring(line.indexOf(',')+1)
				val sentiment = 
					text.split(" ")
						.map(word => {
						 	val res = wordToSentimentMap.get(word.trim.toLowerCase)
						 	if (res == null) (0.0,0.0)
						 	else res													  	 	
					  	})
						.foldRight(0.0,0.0) ((acc:Main.Sentiment, elm:Main.Sentiment) => (acc._1 + elm._1, acc._2 + elm._2))
				val res = if (sentiment._1 < sentiment._2) 0 else if (sentiment._1 == sentiment._2) 1 else 2
				start + ";" + Instant.now.toEpochMilli.toString + ";" + timestamp + "," + text + "\t" + res
			})
			.addSink(kafkaProducer)
		env
	}

	def fillMap(path: Path): Unit = {
		val hdfs = FileSystem.get(path.toUri, new org.apache.hadoop.conf.Configuration())
		val status = hdfs.listStatus(path)
		//Loop through all files in the given directory path
		status.foreach( x =>
			if(!x.getPath.toString.contains("_SUCCESS")) {
				readFile(x.getPath)
			}
		)
	}

	def addWordToMap(input: String): Unit = {
		val values: Array[String] = input.split(",")
		wordToSentimentMap.put(values(0), new Sentiment(values(1).toDouble, values(2).toDouble))
	}

	def readFile(path: Path): Unit ={
		val hdfs = FileSystem.get(path.toUri, new org.apache.hadoop.conf.Configuration())
		val stream = hdfs.open(path) //Opens a file based on the path
		def readLines = Stream.cons(stream.readLine, Stream.continually(stream.readLine))

			var firstLine = true
			readLines.takeWhile(_ != null).foreach(line => {
				if(firstLine)
					firstLine = false
				else
					addWordToMap(line)
			})
	}

	implicit def map2Properties(map: Map[String, String]): java.util.Properties = {
		(new java.util.Properties /: map) { case (props, (k, v)) => props.put(k, v); props }
	}

}