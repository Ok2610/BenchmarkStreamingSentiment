package Sentiment

import java.time.Instant
import java.util

import org.apache.storm.task.{OutputCollector, TopologyContext}
import org.apache.storm.topology.{BasicOutputCollector, IRichBolt, OutputFieldsDeclarer}
import org.apache.storm.topology.base.{BaseBasicBolt, BaseRichBolt}
import org.apache.storm.tuple.{Fields, Tuple, Values}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

class LexiconBolt(benchmarkId: String, hdfsPath: String) extends BaseRichBolt {


  type Sentiment = (Double, Double)

  var collector: OutputCollector = _
  val wordToSentimentMap = new util.HashMap[String, Sentiment]

  override def prepare(stormConf: util.Map[_, _], context: TopologyContext, collector: OutputCollector): Unit = {
    this.collector = collector
    fillMap(new Path(hdfsPath))
  }

  override def execute(input: Tuple): Unit = {
    val inputString = input.getStringByField("str")
    if(benchmarkId.toInt == -1) {
      collector.emit(new Values(analyseSentence(inputString)))
    } else {
      val startTS = Instant.now().toEpochMilli.toString
      val separator = inputString.indexOf(",")
      val timestamp = inputString.substring(0,separator)
      val sentence = inputString.substring(separator+1)
      val result = analyseSentence(sentence)
      val output = startTS + ";" + Instant.now().toEpochMilli + ";" + timestamp + "," + result
      collector.emit(new Values(output))

      //At least once
      //collector.emit(input, new Values(output))
      //collector.ack(input)
    }
  }

  override def declareOutputFields(declarer: OutputFieldsDeclarer): Unit = {
    //declarer.declare(new Fields("key", "message"))
    declarer.declare(new Fields("message"))
  }

  def analyseSentence(sentence: String): String = {
    val totalSentiment = sentence.split(" ")
      .map(w => w.trim)
      .toList
      .map(w => wordToSentimentMap.getOrDefault(w, (0.0,0.0)))
      .foldRight((0.0,0.0)) ((acc: (Double, Double), elm: (Double, Double)) => (acc._1 + elm._1, acc._2 + elm._2))
    if(totalSentiment._1 > totalSentiment._2) {
      sentence + "\t1"
    } else {
      sentence + "\t0"
    }
  }

  def fillMap(path: Path): Unit = {
    val hdfs = FileSystem.get(path.toUri, new Configuration())
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
    val hdfs = FileSystem.get(path.toUri, new Configuration())
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
}
