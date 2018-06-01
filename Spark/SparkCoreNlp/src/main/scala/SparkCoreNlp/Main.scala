package SparkCoreNlp

import java.util
import java.util.Properties
import java.io.File
import com.typesafe.config.{ Config, ConfigFactory }

import scala.collection.JavaConversions._

import org.apache.kafka.clients.producer.ProducerRecord
import com.github.benfradet.spark.kafka.writer._
import kafka.serializer.StringDecoder
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming._

import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations
import edu.stanford.nlp.pipeline.{Annotation, StanfordCoreNLP}
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations

import java.time.Instant

//http://apache-spark-user-list.1001560.n3.nabble.com/Spark-and-Stanford-CoreNLP-tp19654p19658.html
//the transient lazy val pattern means the StanfordCoreNLP will not be serialized and will
//only be calculated once it is accessed (in the workers in this case) and then stored for future reference
object MyCoreNLP{
  val props = new Properties()
  props.setProperty("parse.model", "edu/stanford/nlp/models/srparser/englishSR.ser.gz")
  props.setProperty("annotators", "tokenize,ssplit,pos,parse,sentiment")
  @transient lazy val coreNLP = new StanfordCoreNLP(props) 
}

object Main {

  val config = ConfigFactory.parseFile(new File("/opt/benchmark/application.conf"))

  def main(args: Array[String]) {

    val benchmarkId   = args(0)
    val consumerTopic = args(1)
    val producerTopic = args(2)

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("SparkCoreNlp")
    val spark = SparkSession.builder()
                            .config(sparkConf)
                            .getOrCreate()
    val ssc = new StreamingContext(spark.sparkContext, Milliseconds(config.getLong("cluster.spark.batchInterval")))
    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._
 
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> config.getString("cluster.kafka.brokers"),
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "test_group_id",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array(consumerTopic)
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
    
    
    val producerConfig = Map(
      "bootstrap.servers" -> config.getString("cluster.kafka.brokers"),
      "key.serializer" -> classOf[StringSerializer].getName,
      "value.serializer" -> classOf[StringSerializer].getName
    )

    //val coreNlp = setupCoreNLP

    //The first tuple is the key, which contains timestamp of the instance that the benchmark data was sent to the first Kafka pipeline. The value contains the message of the data. 
    val kafkaRawData = stream.map(record => (record.value, Instant.now.toEpochMilli.toString))

    val totalScoreData = kafkaRawData.map(message => (message._1, message._2, sentimentAnalysis(MyCoreNLP.coreNLP,message._1.substring(message._1.indexOf(",")+1))))
      
    totalScoreData.writeToKafka(
      producerConfig,
      s => new ProducerRecord[String, String](producerTopic, s._2 + ";" + Instant.now.toEpochMilli + ";" + s._1 + "\t" + s._3)  
    )
    
    // Start the computation
    ssc.start
    ssc.awaitTermination
  }

  //def setupCoreNLP: StanfordCoreNLP = {
  //  val props = new Properties()
  //  props.setProperty("annotators", "tokenize, ssplit, parse, sentiment")
  //    new StanfordCoreNLP(props)
  //}

  def sentimentAnalysis(pipeline: StanfordCoreNLP, input: String): String = {
    val sentiment = extractSentiment(pipeline, input).maxBy{ case (s,_) => s.length }
    sentiment._2.toString
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
}

