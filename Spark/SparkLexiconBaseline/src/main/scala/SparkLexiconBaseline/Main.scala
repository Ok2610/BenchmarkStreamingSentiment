package SparkLexiconBaseline

import java.util
import java.io.File
import com.typesafe.config.{ Config, ConfigFactory }

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

import java.time.Instant

case class SentiRow (Word:String, PosScore:Double, NegScore:Double)

object Main {

  val config = ConfigFactory.parseFile(new File("/opt/benchmark/application.conf"))
  
  def main(args: Array[String]) {

    val benchmarkId   = args(0)
    val consumerTopic = args(1)
    val producerTopic = args(2)

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("SparkLexiconBaseline")
    val spark = SparkSession.builder()
                            .config(sparkConf)
                            .getOrCreate()
    val ssc = new StreamingContext(spark.sparkContext, Milliseconds(config.getLong("cluster.spark.batchInterval")))
    spark.sparkContext.setLogLevel("OFF")

    import spark.implicits._

    val schema = StructType(Array(
				StructField("Word", StringType,false),
				StructField("PosScore", DoubleType,false),
				StructField("NegScore", DoubleType,false)))

    val lexiconDS = spark.read.option("header", true).schema(schema).csv("sentiWord_1_avgScores").as[SentiRow]
    
    val lexiconMap = ssc.sparkContext.broadcast(lexiconDS.rdd.map{
      case SentiRow(word, positive, negative) => (word, (positive,negative))
    }.collectAsMap)
    
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

    //The first tuple is the key, which contains timestamp of the instance that the benchmark data was sent to the first Kafka pipeline. The value contains the message of the data. 
    val kafkaRawData = stream.map(record => (record.value, Instant.now.toEpochMilli.toString))

    if (benchmarkId == -1) {
      val totalScoreData = kafkaRawData.map(message =>
          (message._1, message._1.split(" ")
                                  .foldLeft((0.0,0.0)){(acc, word) => 
                                    val scoreTuple = lexiconMap.value.getOrElse(word.toLowerCase, (0.0,0.0))
                                    (acc._1 + scoreTuple._1, acc._2 + scoreTuple._2)
                                  }))
      
      totalScoreData.writeToKafka(
      producerConfig,
      s => 
        if (s._2._1 > s._2._2) {
          new ProducerRecord[String, String](producerTopic, s._1 + "\t" + 2)
        } else {
          new ProducerRecord[String, String](producerTopic, s._1 + "\t" + 0)
        }
    )
    } else {
      val totalScoreData = kafkaRawData.map(message =>
          (message._1, message._2, message._1.substring(message._1.indexOf(",")+1).split(" ")
                                  .foldLeft((0.0,0.0)){(acc, word) => 
                                    val scoreTuple = lexiconMap.value.getOrElse(word.toLowerCase, (0.0,0.0))
                                    (acc._1 + scoreTuple._1, acc._2 + scoreTuple._2)
                                  }))
      totalScoreData.writeToKafka(
      producerConfig,
      s => 
        if (s._3._1 > s._3._2) {
          new ProducerRecord[String, String](producerTopic, s._2 + ";" + Instant.now.toEpochMilli.toString + ";" + s._1 + "\t" + 2)
        } else {
          new ProducerRecord[String, String](producerTopic, s._2 + ";" + Instant.now.toEpochMilli.toString + ";" + s._1 + "\t" + 0)
        }
    )
    }
    
    // Start the computation
    ssc.start
    ssc.awaitTermination
  }
}

