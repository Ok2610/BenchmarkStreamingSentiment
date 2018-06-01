package FlinkCoreNlp

import java.util.Properties
import java.io.File
import java.time.Instant

import com.typesafe.config.{ Config, ConfigFactory }

import scala.collection.JavaConversions._

import org.apache.flink.api.common.typeinfo
import org.apache.flink.api.common.state.ValueState
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.common.functions.RichMapFunction

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.flink.streaming.util.serialization

import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer010, FlinkKafkaProducer010}

import org.apache.flink.configuration.Configuration

import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations
import edu.stanford.nlp.pipeline.{Annotation, StanfordCoreNLP}
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations

object Main {

	case class Message(key:String,value:String)

	//http://apache-spark-user-list.1001560.n3.nabble.com/Spark-and-Stanford-CoreNLP-tp19654p19658.html
	//the transient lazy val pattern means the StanfordCoreNLP will not be serialized and will
	//only be calculated once it is accessed (in the workers in this case) and then stored for future reference
	object MyCoreNLP{
		val props = new Properties()
		props.setProperty("parse.models", "edu/stanford/nlp/models/srparser/englishSR.ser.gz")
		props.setProperty("annotators", "tokenize,ssplit,pos,parse,sentiment")
		@transient lazy val coreNLP = new StanfordCoreNLP(props) 
	}

	val config = ConfigFactory.parseFile(new File("/opt/benchmark/application.conf"))

	def main(args: Array[String]): Unit = {
		
		val benchmarkId = args(0).toInt
		val consumerTopic = args(1)
		val producerTopic = args(2)
		val brokers = config.getString("cluster.kafka.brokers")
		val zookeeper = config.getString("cluster.zookeeper.hosts")
		val consumerGroupId = "flink"

		val env = StreamExecutionEnvironment.getExecutionEnvironment

		val par = config.getInt("cluster.flink.yarn.slots")

		env.setParallelism(par)

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
				.map(msg => (Instant.now.toEpochMilli.toString,msg,Instant.now.toEpochMilli % par))
				.keyBy(2)
				.map(new RichMapFunction[(String,String,Long), String] {
	  				var coreNlpState: ValueState[StanfordCoreNLP] = _

			  		override def open(config: Configuration) {
	    				val coreNlpDesc: ValueStateDescriptor[StanfordCoreNLP] = new ValueStateDescriptor[StanfordCoreNLP]("corenlp", classOf[StanfordCoreNLP])
						coreNlpState = getRuntimeContext.getState(coreNlpDesc)
					}

					override def map(in: (String,String,Long)): String = {
	    				if (coreNlpState.value == null) {
							val props = new Properties()
							props.setProperty("parse.models", "edu/stanford/nlp/models/srparser/englishSR.ser.gz")
							props.setProperty("annotators", "tokenize,ssplit,parse,sentiment")
		    				coreNlpState.update(new StanfordCoreNLP(props))
						}
	    		
			    		val startTime = in._1
						val splitIndex = in._2.indexOf(",")
						val timestamp = in._2.substring(0, splitIndex)
						val sentence = in._2.substring(splitIndex + 1)
						val sentiment = sentimentAnalysis(coreNlpState.value, sentence)._2.toString
						return startTime + ";" + Instant.now.toEpochMilli.toString + ";" + timestamp + "," + sentence + "\t" + sentiment
	  				}
				})
				.addSink(kafkaProducer)
				//.map((in) => {
				//	val startTime = Instant.now.toEpochMilli.toString
				//	val splitIndex = in.indexOf(",")
				//	val timestamp = in.substring(0, splitIndex)
				//	val sentence = in.substring(splitIndex + 1)
				//	val sentiment = sentimentAnalysis(MyCoreNLP.coreNLP, sentence)._2.toString
				//	startTime + ";" + Instant.now.toEpochMilli.toString + ";" + timestamp + "," + sentence + "\t" + sentiment
				//})

		env.execute
	}

	def sentimentAnalysis(pipeline: StanfordCoreNLP, input: String): (String, Int) = {
		extractSentiment(pipeline, input).maxBy{ case (s,_) => s.length }
	}

	private def extractSentiment(pipeline: StanfordCoreNLP, text: String): List[(String,Int)] = {
		val annotation: Annotation = pipeline.process(text)
		val sentences = annotation.get(classOf[CoreAnnotations.SentencesAnnotation])
		sentences
			.map(s => (s, s.get(classOf[SentimentCoreAnnotations.SentimentAnnotatedTree])))
			.map{
				case (s, t) => (s.toString, getSentiment(RNNCoreAnnotations.getPredictedClass(t)))
			}
			.toList
	}

	private def getSentiment(sentiment: Int): Int = sentiment match {
		case x if x < 2 => 0
		case 2 => 1
		case x if x > 2 => 2
	}

	implicit def map2Properties(map: Map[String, String]): java.util.Properties = {
		(new java.util.Properties /: map) { case (props, (k, v)) => props.put(k, v); props }
	}

	class SentimentState extends RichMapFunction[String, String] {

  		var coreNlpState: ValueState[StanfordCoreNLP] = _

  		override def open(config: Configuration) {
    		// configure state
    		val coreNlpDesc: ValueStateDescriptor[StanfordCoreNLP] = new ValueStateDescriptor[StanfordCoreNLP]("corenlp", classOf[StanfordCoreNLP])
			coreNlpState = getRuntimeContext.getState(coreNlpDesc)
		}

		override def map(in: String): String = {
    		if (coreNlpState.value == null) {
				val props = new Properties()
				props.setProperty("parse.models", "edu/stanford/nlp/models/srparser/englishSR.ser.gz")
				props.setProperty("annotators", "tokenize,ssplit,pos,parse,sentiment")
    			coreNlpState.update(new StanfordCoreNLP(props))
    		}
    		
    		val startTime = Instant.now.toEpochMilli.toString
			val splitIndex = in.indexOf(",")
			val timestamp = in.substring(0, splitIndex)
			val sentence = in.substring(splitIndex + 1)
			val sentiment = sentimentAnalysis(coreNlpState.value, sentence)._2.toString
			startTime + ";" + Instant.now.toEpochMilli.toString + ";" + timestamp + "," + sentence + "\t" + sentiment
  		}
	}
}