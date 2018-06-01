package Sentiment

import java.util.{Properties, UUID}
import java.io.File
import com.typesafe.config.{ Config, ConfigFactory }

import org.apache.storm.{Config, StormSubmitter}
import org.apache.storm.kafka.{KafkaSpout, SpoutConfig, StringScheme, ZkHosts}
import org.apache.storm.kafka.bolt.KafkaBolt
import org.apache.storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper
import org.apache.storm.kafka.bolt.selector.DefaultTopicSelector
import org.apache.storm.spout.SchemeAsMultiScheme
import org.apache.storm.topology.TopologyBuilder

object Sentiment {
  def main(args: Array[String]): Unit = {

    if(args(0) == null || args(1) == null || args(2) == null ){
      println("MISSING ARGUMENTS")
      return
    }

    val benchmarkId = args(0)
    val inputTopic = args(1)
    val outputTopic = args(2)
    val config = ConfigFactory.parseFile(new File("/opt/benchmark/application.conf"))

    //Get parallelism settings
    val workers = config.getInt("cluster.storm.workers")
    val spoutThreads = config.getInt("cluster.storm.spoutThreads")
    val sentimentThreads = config.getInt("cluster.storm.sentimentThreads")
    val boltThreads = config.getInt("cluster.storm.boltThreads")

    //Define properties for kafka connection
    val props = new Properties()
    val brokers = config.getString("cluster.kafka.brokers")
    props.put("bootstrap.servers", brokers)
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")


    //Setup kafka spout - it outputs strings with the field name "str"
    val zookeeper = config.getString("cluster.zookeeper.hosts")
    val hosts = new ZkHosts(zookeeper)
    val spoutConfig = new SpoutConfig(hosts, inputTopic, "/" + inputTopic, "storm-consumption")
    spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme)
    spoutConfig.ignoreZkOffsets = true
    spoutConfig.startOffsetTime = kafka.api.OffsetRequest.LatestTime
    val kafkaSpout = new KafkaSpout(spoutConfig)


    //KafkaBolt expects two incoming fields to be "key" and "message"
    val kafkaBolt = new KafkaBolt()
      .withProducerProperties(props)
      .withTopicSelector(new DefaultTopicSelector(outputTopic))
      .withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper())

    val kafkaSpoutId = "spout"
    val sentimentBoltId = "coreNlp"
    val kafkaBoltId = "sendToKafka"

    val builder = new TopologyBuilder
    builder.setSpout(kafkaSpoutId, kafkaSpout, spoutThreads)

    builder.setBolt(sentimentBoltId, new CoreNlpBolt(benchmarkId), sentimentThreads).shuffleGrouping(kafkaSpoutId)

    builder.setBolt(kafkaBoltId, kafkaBolt, boltThreads).shuffleGrouping(sentimentBoltId)

    val conf = new org.apache.storm.Config()
    conf.put("bootstrap.servers", brokers)
    conf.put("kafka.broker.properties", props)
    conf.put(org.apache.storm.Config.TOPOLOGY_ACKER_EXECUTORS, new Integer(0)) //Disable reliability

    conf.setNumWorkers(workers)
    StormSubmitter.submitTopology("Sentiment", conf, builder.createTopology())
  }
}
