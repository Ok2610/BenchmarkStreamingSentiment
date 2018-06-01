package FlinkLexiconFilestream

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
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}

object Main {

	type Sentiment = (Double, Double)
	type FileLine = (String, Sentiment)

	case class Message(key:String,value:String)

	val config = ConfigFactory.parseFile(new File("/opt/benchmark/application.conf"))

	def main(args: Array[String]): Unit = {
		
		val benchmarkId = args(0).toInt
		val consumerTopic = args(1)
		val producerTopic = args(2)
		val brokers = config.getString("cluster.kafka.brokers")
		val zookeeper = config.getString("cluster.zookeeper.hosts")
		val consumerGroupId = "flink"

		val env = StreamExecutionEnvironment.getExecutionEnvironment

		env.setParallelism(config.getInt("cluster.flink.yarn.slots"))

		val fileStream = 
				env.readTextFile("hdfs:" + config.getString("cluster.hdfs.lexiconPath"))
					.filter(!_.contains("PosScore")) //Skip header
			  		.map(line => {
			  			val splitLine = line.split(",")
			  			("file", new FileLine(splitLine(0).toString, new Sentiment(splitLine(1).toDouble, splitLine(2).toDouble)))
			  		})

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
			new SimpleStringSchema//new KeyValueOutputSchema
		)

		val kafkaStream = env.addSource(kafkaConsumer)
		
		val wordStream = 
			kafkaStream
			.map(line => {
				("file", Instant.now.toEpochMilli.toString, line.substring(0, line.indexOf(',')), line.substring(line.indexOf(',')+1))
			})
			.connect(fileStream)
			.keyBy(_._1,_._1)
			.map(new RichCoMapFunction[(String, String, String, String), (String,FileLine), String] {
				var fileMapState: MapState[String,Sentiment] = _

				override def open(parameters: Configuration): Unit = {
					fileMapState = getRuntimeContext.getMapState(new MapStateDescriptor[String,Sentiment]("fileMap", classOf[String], classOf[Sentiment]))
				}

				def map1(in:(String, String, String, String)): String = {
					val sentiment = 
						in._4.split(" ")
							 .map(word => {
						  	 	val res = fileMapState.get(word.trim.toLowerCase)
						  	 	if (res == null) (0.0,0.0)
						  	 	else res													  	 	
					  		 })
						  	 .foldRight(0.0,0.0) ((acc:Main.Sentiment, elm:Main.Sentiment) => (acc._1 + elm._1, acc._2 + elm._2))
					val res = if (sentiment._1 < sentiment._2) 0 else if (sentiment._1 == sentiment._2) 1 else 2
					in._2 + ";" + Instant.now.toEpochMilli.toString + ";" + in._3 + "," + in._4 + "\t" + res
				}

				def map2(in:(String,FileLine)): String = {
					fileMapState.put(in._2._1, in._2._2)
					""
				}
			})
			.filter(res => res != "")
			.addSink(kafkaProducer)
		env.execute
	}

	implicit def map2Properties(map: Map[String, String]): java.util.Properties = {
		(new java.util.Properties /: map) { case (props, (k, v)) => props.put(k, v); props }
	}

}