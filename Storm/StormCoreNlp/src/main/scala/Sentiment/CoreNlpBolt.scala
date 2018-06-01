package Sentiment

import java.util

import org.apache.storm.task.{OutputCollector, TopologyContext}
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.topology.base.BaseRichBolt
import org.apache.storm.tuple.{Fields, Tuple, Values}
import java.time.Instant
import java.util
import java.util.Properties
import scala.collection.JavaConversions._

import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations
import edu.stanford.nlp.pipeline.{Annotation, StanfordCoreNLP}
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations

class CoreNlpBolt(benchmarkId: String) extends BaseRichBolt {

  var collector: OutputCollector = _
  var coreNlp: StanfordCoreNLP = _

  override def prepare(stormConf: util.Map[_, _], context: TopologyContext, collector: OutputCollector): Unit = {
    this.collector = collector
    this.coreNlp = setupCoreNLP
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
    }
  }

  override def declareOutputFields(declarer: OutputFieldsDeclarer): Unit = {
    declarer.declare(new Fields("message"))
  }

  def analyseSentence(sentence: String): String = {
    val (_, sentiment:Int) = sentimentAnalysis(coreNlp, sentence)
    sentence + "\t" + sentiment
  }

  def setupCoreNLP: StanfordCoreNLP = {
    val props = new Properties()
    props.setProperty("parse.model", "edu/stanford/nlp/models/srparser/englishSR.ser.gz")
    props.setProperty("annotators", "tokenize,ssplit,pos,parse,sentiment")
    new StanfordCoreNLP(props)
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
}
