package streambench

import java.nio.file.{Files, Paths, StandardOpenOption}
import java.io.File

import com.typesafe.config.{ Config, ConfigFactory }

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}

import scala.io.Source
import java.util.Random
import java.util.concurrent._

import sys.process._
import scala.collection.JavaConverters._
import java.util.{Collections, Properties}

import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import java.time.Instant
import java.lang.Long
import java.util.concurrent.{ExecutorService, Executors}

import scala.concurrent.duration._
import scala.concurrent.Future
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global

import scala.language.postfixOps
import util.control.Breaks._
import java.util

object Main {

  case class DelayStep(benchmarkId: String, start:Long, end:Long, delay: Long, wave: String, frameworkStart: String, frameworkEnd: String)

  val config = ConfigFactory.parseFile(new File("/opt/benchmark/application.conf"))

  var scheduler: ScheduledExecutorService = Executors.newScheduledThreadPool(1)

  var producerDoneTimestamp = 0L
  var producerDataArray: Array[String] = _
  val random = new Random

  var driverProducerTopic = ""
  var driverConsumerTopic = ""
  var jarFile = ""
  var idFileEntry = ""
  var messageType = ""
  var kafkaMessage = ""
  var realMessageCounter = 0
  
  
  def main(args: Array[String]):Unit = {
    if(args.length < 10) {
      println("Missing arguments. Example run: \njava -jar benchdriver.jar [platform] [jarFile] [messagesPerWave] [numWaves] [numProducers] [numConsumers] [kafkaReplicationFactor] [kafkaPartitions] [messageType] [description@with@spaces] [waveInterval]")
      return
    }

    val platform = args(0).toLowerCase // platform to be benchmarked
    jarFile = args(1) //Name of dataFramework jarfile located in /jars/ directory
    val sendRatePerWave = args(2).toInt // the amount of messages per second being sent by each thread
    val numWaves = args(3).toInt // number of producer waves. add 1 for warm up
    val numProducers = args(4).toInt // the amount of Kafka producers used for producing messages
    val numConsumers = args(5).toInt //
    val kafkaReplicationFactor = args(6).toInt //How many kafka replications
    val kafkaPartitions = args(7).toInt //How many kafka partition
    val messageType = args(8) //What type of message should be sent
    val benchmarkDescription = args(9).replaceAll("@", " ") // Short text@with@spaces describing the benchmark
    val waveInterval = args(10).toInt

    val benchmarkId = setupNewBenchmark()

    kafkaMessage = messageType match {
      case "small" => "Hello" //5 bytes
      case "medium" => List.fill(47)("Hello").mkString(" ") //280 bytes (Max tweet size)
      case "large" => List.fill(170667)("Hello").mkString(" ") //1mb
      case _ => {
        producerDataArray = Source.fromFile(config.getString("extra.realFileAbsolutePath")).getLines.toArray
        ""
      }
    }
    scheduler = Executors.newScheduledThreadPool(numProducers)

    driverProducerTopic = "Producer" + benchmarkId
    driverConsumerTopic = "Consumer" + benchmarkId

    createKafkaTopics(driverProducerTopic, kafkaPartitions, kafkaReplicationFactor)
    createKafkaTopics(driverConsumerTopic, kafkaPartitions, kafkaReplicationFactor)

    val benchmarkParameters = benchmarkDescription + ";" + platform + ";" + driverProducerTopic + ";" + driverConsumerTopic + ";" + sendRatePerWave + ";" + numWaves + ";" + numProducers
    
    idFileEntry = idFileEntry + benchmarkId+","+benchmarkParameters

    println("Running benchmark with: \n platform: " + platform + "\n driverProducerTopic: " + driverProducerTopic + "\n driverConsumerTopic: " + driverConsumerTopic + "\n sendRatePerWave: " + sendRatePerWave + "\n numWaves: " + numWaves + "\n numProducers: " + numProducers)
    
    val producerArray = new Array[KafkaProducer[String,String]](numProducers)
    for (i <- 0 until numProducers) {
      producerArray(i) = setupKafkaProducer("BenchmarkProducer"+i)
    }

    val futures = List

    val consumerArray = new Array[KafkaConsumer[String,String]](numConsumers)
    for (i <- 0 until numConsumers) {
      consumerArray(i) = setupKafkaConsumer()
    }
    
    val commandLineParameters = " " + benchmarkId + " " + driverProducerTopic + " " + driverConsumerTopic
    if (platform.equals("spark")) {
      startSpark(commandLineParameters)
      Thread.sleep(config.getString("extra.sparkStartupWaitTime").toInt)
    } else if (platform.equals("flink")) {
      startFlink(commandLineParameters)
      Thread.sleep(config.getString("extra.flinkStartupWaitTime").toInt)
    } else if (platform.equals("storm")) {
      startStorm(commandLineParameters)
      Thread.sleep(config.getString("extra.stormStartupWaitTime").toInt)
    } else if (platform.equals("kafka")) {
      driverProducerTopic = driverConsumerTopic
    }

    val warmUpFutures = runBenchmark(numProducers, numConsumers, benchmarkId, sendRatePerWave, numWaves, waveInterval, true)
    val warmUpResults = Await.result(warmUpFutures, Duration.Inf)
    println("Warm up done")

    val consumerFutures = runBenchmark(numProducers, numConsumers, benchmarkId, sendRatePerWave, numWaves, waveInterval, false)
    println("Awaiting results")
    val consumerResults = Await.result(consumerFutures, Duration.Inf)
    println("ALL CONSUMERS DONE")

    Files.write(Paths.get("id.txt"), (idFileEntry + "," +Instant.now.toEpochMilli.toString+"\n").getBytes(), StandardOpenOption.APPEND)
    writeDelaysToFile(consumerResults.flatten.toList)

    if (platform.equals("spark")) {
      killSparkApplication
    } else if (platform.equals("flink")) {
      killFlinkApplication
    } else if (platform.equals("storm")) {
      killStormApplication
    }
    resetKafka(driverProducerTopic, driverConsumerTopic)
  }

  def setupKafkaConsumer(): KafkaConsumer[String, String] = {
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString("cluster.kafka.brokers"))
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "BenchmarkDriver")
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
    props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000")
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000")
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    return new KafkaConsumer[String, String](props)
  }

  def startConsumers(consumers: Array[KafkaConsumer[String,String]], topic: String, benchmarkId: String, sendRatePerWave: Int, numWaves: Int, waveInterval:Int, warmUp: Boolean): Future[Seq[List[DelayStep]]] = {
    val futures = for (i <- 0 until consumers.length) yield Future {
      var futureDelayList = List[DelayStep]()
      val consumer = consumers(i)
      consumer.subscribe(Collections.singletonList(topic))
      var consumeCount = 0
      var lastConsumedTimestamp = Instant.now.toEpochMilli
      breakable {
        while (true) {
          val records = consumer.poll(100).asScala
          for (record <- records) {
            val frameworkStartTimestamp = record.value().substring(0, record.value().indexOf(";"))
            val frameworkEndTimestamp = record.value().substring(frameworkStartTimestamp.length + 1, record.value().indexOf(";", frameworkStartTimestamp.length + 1))
            val wave = record.value().substring(frameworkStartTimestamp.length + frameworkEndTimestamp.length + 2, record.value().indexOf(";", frameworkStartTimestamp.length + frameworkEndTimestamp.length + 2))
            val benchTimestamp = record.value().substring(frameworkStartTimestamp.length + frameworkEndTimestamp.length + wave.length + 3, record.value().indexOf(","))
            val message = record.value().substring(frameworkStartTimestamp.length + frameworkEndTimestamp.length + wave.length+benchTimestamp.length + 4)
            val currentTimestamp = Instant.now.toEpochMilli
            lastConsumedTimestamp = currentTimestamp
            
            if (!warmUp) {
              val delay = currentTimestamp - Long.parseLong(benchTimestamp)
              futureDelayList = saveDelay(futureDelayList, benchmarkId, Long.parseLong(benchTimestamp), currentTimestamp, delay, wave, frameworkStartTimestamp, frameworkEndTimestamp)
              consumeCount = consumeCount + 1
            }                                                                                                     
          }
          if (Instant.now.toEpochMilli - lastConsumedTimestamp > 30000+waveInterval) {
            break
          }
        }
      }
      println("Consumer has consumed " + consumeCount + " messages.")
      consumer.close
      println("CONSUMER STOPPED")
      futureDelayList
    }

    Future.sequence(futures)
  }

  def setupKafkaProducer(clientId: String): KafkaProducer[String, String] = {
    val props = new Properties()
    props.put("bootstrap.servers", config.getString("cluster.kafka.brokers"))
    props.put("client.id", clientId)
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    return new KafkaProducer[String, String](props)
  }

  def producerRunner(topic: String, producer: KafkaProducer[String, String], sendRate: Int, numWaves: Int, waveInterval: Int): Thread = {
    new Thread(() => { //Don't change to anon function
      var waveCount = 0
      while (waveCount < numWaves) {
        for (i <- 1 to sendRate) {
          val data = new ProducerRecord[String, String](topic, waveCount + ";" + String.valueOf(Instant.now.toEpochMilli) + "," + getMessage)
          producer.send(data)
        }
        waveCount = waveCount + 1
        Thread.sleep(waveInterval)         
      }
      producer.close
    })
  }

  def getMessage: String ={
    if(kafkaMessage == "") {
      return producerDataArray(realMessageCounter % producerDataArray.length)
    }
    kafkaMessage
  }

  def runBenchmark(numProducers:Int, numConsumers:Int, benchmarkId:String, sendRatePerWave:Int, numWaves:Int, waveInterval:Int, warmUp: Boolean): Future[Seq[List[DelayStep]]] = {
    val producerArray = new Array[KafkaProducer[String,String]](numProducers)
    val consumerArray = new Array[KafkaConsumer[String,String]](numConsumers)
    val futures = List
    val initialDelay = 1 // initial delay before producer starts sending
    val rateOfSend = 1 // rate at which producer sends, e.g. every second, every 2 seconds etc.
    val producerRunnerArray = new Array[Thread](numProducers)
    
    for (i <- 0 until numProducers) {
      producerArray(i) = setupKafkaProducer("BenchmarkProducer"+i)
    }

    for (i <- 0 until numConsumers) {
      consumerArray(i) = setupKafkaConsumer()
    }

    if (warmUp) {
      val consumerFutures = startConsumers(consumerArray, driverConsumerTopic, benchmarkId, sendRatePerWave, 1, 300000, true)
      for (i <- 0 until numProducers) {
        producerRunnerArray(i) = producerRunner(driverProducerTopic, producerArray(i), sendRatePerWave, 1, 0)
      }

      for (producerRunner <- producerRunnerArray) {
        producerRunner.start
      }

      consumerFutures
    } else {
      println("Starting new Consumers")
      val consumerFutures = startConsumers(consumerArray, driverConsumerTopic, benchmarkId, sendRatePerWave, numWaves, waveInterval, false)    
      println("Starting new Producers")
      for (i <- 0 until numProducers) {
        producerRunnerArray(i) = producerRunner(driverProducerTopic, producerArray(i), sendRatePerWave, numWaves, waveInterval)
      }

      idFileEntry = idFileEntry + "," + Instant.now.toEpochMilli.toString
      
      for (producerRunner <- producerRunnerArray) {
        producerRunner.start
      }

      consumerFutures
    }   
  }


  def startSpark(parameters: String) = {
    val sparkThread = new Thread(() => {
      val sparkClass = jarFile.substring(0,jarFile.indexOf(".")) + ".Main"
      (config.getString("cluster.spark.path") + "spark-submit --master yarn --deploy-mode cluster --class " + sparkClass + " ./jars/" + jarFile + parameters).!
    })
    sparkThread.start
  }

  def startFlink(parameters: String) {
    val flinkThread = new Thread(() => {
      (config.getString("cluster.flink.path") + "flink run -m yarn-cluster -yn " + config.getString("cluster.flink.yarn.numberOfTaskManagers") + " -ytm " + config.getString("cluster.flink.yarn.taskManagerMemory") + " -p " + config.getString("cluster.flink.yarn.slots") + " ./jars/"+ jarFile + parameters).!
    })
    flinkThread.start
  }

  def startStorm(parameters: String): Unit = {
	("storm jar ./jars/" + jarFile + " Sentiment.Sentiment" + parameters).!
  }

  def killSparkApplication() = {
    val sparkClass = jarFile.substring(0,jarFile.indexOf(".")) + ".Main"
    killYarnApplicationByAppName(sparkClass)
  }

  def killFlinkApplication() = {
    val flinkClass = jarFile.substring(0,jarFile.indexOf(".")) + ".Main"
    killYarnApplicationByAppName(flinkClass)
  }

  def killStormApplication() = {
    println("killing storm")
    "storm kill Sentiment".!
  }

  def killYarnApplicationByAppName(appName: String) = {
    ("yarn application -kill " + getAppIdByAppName(appName)).!
  }

  def createKafkaTopics(name: String, partitions: Int, replicationFactor: Int) {
    ("sh scripts/create.sh " + replicationFactor + " " + partitions + " " + name).!
  }

  def resetKafka(driverProducerTopic: String, driverConsumerTopic: String) = {
    ("sh scripts/delete.sh " + driverProducerTopic).!
    ("sh scripts/delete.sh " + driverConsumerTopic).!
  }

  def getAppIdByAppName(appName: String): String = {
    val yarnApps = "yarn application -list".!!
    val lines: Array[String] = yarnApps.split("\n")

    //Wish that the YarnClient library worked correctly
    var counter = 0
    var appLine = ""
    lines.foreach(line => {
      if(counter > 1 && appLine.equals("")) {
        if(line.contains(appName)){
          appLine = line
        }
      }
      counter = counter +1
    })

    if(appLine.equals("")){
      println("could not find yarn app")
      return ""
    }

    appLine.split("\t").head
  }

  def saveDelay(delayList: List[DelayStep], benchmarkId: String, start:Long, end:Long, delay: Long, wave: String, frameworkStart: String, frameworkEnd: String): List[DelayStep] = {
    DelayStep(benchmarkId, start, end, delay, wave, frameworkStart, frameworkEnd)::delayList
  }

  def writeDelaysToFile(delayList: List[DelayStep]) = {
    println("Delays in total " + delayList.length)
    val benchmarkId = delayList.head.benchmarkId

    delayList.foreach(delay => {
      val str = delay.benchmarkId + "," + delay.delay + "," + delay.start + "," + delay.end + "," + delay.wave + "," + delay.frameworkStart + "," + delay.frameworkEnd
      Files.write(Paths.get("delay.txt"), (str+"\n").getBytes(), StandardOpenOption.APPEND)
    })
  }

  def setupNewBenchmark(): String = {
    var id = 0
    val idList = Source.fromFile("id.txt").getLines.toList
    println("IDLIST LENGTH: " + idList.length)
    if (idList.isEmpty) {
      return "1"
    }
    for (line <- idList) {
         
      id = line.split(",").head.toInt
    }
    id = id + 1 
    id.toString
  }
}
