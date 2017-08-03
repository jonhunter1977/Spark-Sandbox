import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.json4s.JsonAST.{JString, JValue}
import org.json4s.jackson.JsonMethods._

object TwitterKafkaStreaming {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("twitter-kafka-streaming").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(5))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "192.168.10.4:9092,192.168.10.4:9093,192.168.10.4:9094",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "SparkStreamingConsumer",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("twitter-stream")
    val messages = KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams))

    val tweets = messages
      .map(message => parseMessageJson(message.value))
      .filter(message => filterToLanguage(message, "en"))
      .map(message => getTweet(message))

    tweets.print()

    ssc.start()
    ssc.awaitTermination()
  }

  def parseMessageJson(message: String): JValue = {
    val json = raw"""$message"""
    parse(json, useBigDecimalForDouble = true)
  }

  def filterToLanguage(message: JValue, language: String): Boolean = {
    val lang = (message \ "lang").values
    if(lang == language) true else false
  }

  def getTweet(message: JValue): String = {
    (message \ "text").values.toString
  }
}



